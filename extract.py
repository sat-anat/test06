# -*- coding: utf-8 -*-
"""
スクショウ計算機（https://asmape0104.github.io/scshow-calculator/）から
全カード情報を抽出して CSV を出力する高速版スクリプト。

- HTTP キャッシュ: requests-cache (1日 = 86400秒)
- 並列取得: JS 資産を N 並列で取得
- payload(_payload.json) を最優先し、不十分な場合は JS 資産へフォールバック
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Iterable, List, Dict, Any, Optional, Tuple
import urllib.parse as up
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import requests_cache

# requests-cache: .requests_cache/http-cache.sqlite に保存
requests_cache.install_cache(".requests_cache/http-cache", expire_after=86400)

# ────────────────────────────────────────────────────────────
# Nuxt / assets 抽出用の正規表現（named group を確実に）
# ────────────────────────────────────────────────────────────
# 例:
# <script>window.__NUXT__.config= {public:{},app:{baseURL:"/scshow-calculator/",buildId:"cae223f9...",buildAssetsDir:"/_nuxt/",cdnURL:""}};</script>
NUXT_APP_RE = re.compile(
    r'window\.__NUXT__\.config\s*=\s*\{[^{}]*app\s*:\s*\{[^}]*'
    r'baseURL"\s*:\s*"(?P<baseURL>[^"]+)"[^}]*'
    r'buildId"\s*:\s*"(?P<buildId>[^"]+)"[^}]*'
    r'buildAssetsDir"\s*:\s*"(?P<buildAssetsDir>[^"]+)"',
    re.S,
)

# 例: <script type="application/json" ... id="__NUXT_DATA__" data-src="/scshow-calculator/_payload.json?xxxx">
NUXT_DATA_SRC_RE = re.compile(
    r'id="__NUXT_DATA__"[^>]*\sdata-src="(?P<src>[^"]+_payload\.json[^"]*)"',
    re.S,
)

# index.html の <script / <link> から /_nuxt/*.js を拾う
MODULE_HREF_RE = re.compile(r'href="([^"]*/_nuxt/[^"]+\.js)"')
ALL_JS_PATH_RE = re.compile(r'/_nuxt/[^"\'\s]+\.js')


# ────────────────────────────────────────────────────────────
# カード検出ヒント
# ────────────────────────────────────────────────────────────
SKILL_DSL_HINTS = [
    "ap_up (", "score_up (", "vol_buff (", "score_buff (",
    "vol_up (", "reset ()", "splice ()", "appeal_up (", "ap_reduce (", "cooltime_reduce ("
]
MEMBER_HINTS = ["花帆", "さやか", "梢", "綴理", "瑠璃乃", "慈", "吟子", "小鈴", "姫芽", "セラス", "泉"]

HEADERS = [
    "id", "title", "member", "rarity", "ap_cost",
    "smile", "pure", "cool", "mental",
    "skill_code", "center_skill_code", "center_trait_code",
    "source_asset", "last_seen",
]


@dataclass
class CardRow:
    id: Optional[str]
    title: Optional[str]
    member: Optional[str]
    rarity: Optional[str]
    ap_cost: Optional[int]
    smile: Optional[int]
    pure: Optional[int]
    cool: Optional[int]
    mental: Optional[int]
    skill_code: Optional[str]
    center_skill_code: Optional[str]
    center_trait_code: Optional[str]
    source_asset: str
    last_seen: str


# ────────────────────────────────────────────────────────────
# HTTP
# ────────────────────────────────────────────────────────────
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "card-extractor/1.3 (+https://asmape0104.github.io/scshow-calculator/)",
        "Accept": "*/*",
        "Accept-Language": "ja,en;q=0.9",
        "Connection": "keep-alive",
    })
    return s


def fetch(url: str, session: Optional[requests.Session] = None, retries: int = 2) -> str:
    s = session or _session()
    last_exc = None
    for attempt in range(retries + 1):
        try:
            r = s.get(url, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_exc = e
            time.sleep(0.6 * (attempt + 1))
    raise last_exc  # type: ignore


def fetch_many(urls: List[str], session: Optional[requests.Session] = None, max_workers: int = 8) -> Dict[str, Optional[str]]:
    """URL群を並列取得（max_workers=8）。失敗は None。"""
    s = session or _session()
    out: Dict[str, Optional[str]] = {u: None for u in urls}
    if not urls:
        return out
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(fetch, u, s): u for u in urls}
        for fut in as_completed(futs):
            u = futs[fut]
            try:
                out[u] = fut.result()
            except Exception:
                out[u] = None
    return out


# ────────────────────────────────────────────────────────────
# Nuxt assets / payload の検出
# ────────────────────────────────────────────────────────────
def _extract_meta_from_index(html: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """baseURL / buildId / buildAssetsDir を index.html から抽出"""
    m = NUXT_APP_RE.search(html)
    if not m:
        return (None, None, None)
    return (m.group("baseURL"), m.group("buildId"), m.group("buildAssetsDir"))


def _extract_payload_url(html: str, base_url: str) -> Optional[str]:
    m = NUXT_DATA_SRC_RE.search(html)
    if not m:
        return None
    path = m.group("src")
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return up.urljoin(base_url, path.lstrip("/"))


def discover_assets_and_payload(base_url: str, session: Optional[requests.Session] = None) -> Tuple[List[str], Optional[str]]:
    index_url = up.urljoin(base_url, "index.html")
    html = fetch(index_url, session=session)

    payload_url = _extract_payload_url(html, base_url)
    js_urls: set[str] = set()

    baseURL, buildId, buildAssetsDir = _extract_meta_from_index(html)
    # /_nuxt/builds/meta/<buildId>.json を辿れる場合はそこからも列挙
    if buildId and buildAssetsDir:
        meta_url = up.urljoin(base_url, f"{buildAssetsDir.lstrip('/')}/builds/meta/{buildId}.json")
        try:
            meta_text = fetch(meta_url, session=session)
            meta = json.loads(meta_text)

            def walk(v):
                if isinstance(v, str) and v.endswith(".js") and ("/_nuxt/" in v or "/__nuxt/" in v):
                    js_urls.add(up.urljoin(base_url, v.lstrip("/")))
                elif isinstance(v, list):
                    for x in v:
                        walk(x)
                elif isinstance(v, dict):
                    for x in v.values():
                        walk(x)
            walk(meta)
        except Exception:
            pass

    for href in MODULE_HREF_RE.findall(html) + ALL_JS_PATH_RE.findall(html):
        js_urls.add(up.urljoin(base_url, href.lstrip("/")))

    return (sorted(js_urls), payload_url)


# ────────────────────────────────────────────────────────────
# JS/JSON 抽出
# ────────────────────────────────────────────────────────────
JSON_PARSE_CALL_RE = re.compile(r'JSON\.parse\(\s*([\'"])(?P<body>(?:\\.|(?!\1).)*)\1\s*\)')
QUOTED_JSON_ARRAY_RE = re.compile(r'([\'"])(\[\s*\{.*?\}\s*\])\1', re.S)
BALANCED_ARRAY_START_RE = re.compile(r'\[')


def _decode_js_string_literal(s: str) -> str:
    return json.loads(f'"{s}"')


def _balanced_slice(src: str, start_idx: int, open_ch: str, close_ch: str) -> Optional[str]:
    depth = 0
    i = start_idx
    in_str = False
    esc = False
    quote = ''
    while i < len(src):
        ch = src[i]
        if in_str:
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif ch == quote:
                in_str = False
        else:
            if ch in ("'", '"'):
                in_str = True
                quote = ch
            elif ch == open_ch:
                depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    return src[start_idx:i + 1]
        i += 1
    return None


def iter_candidate_jsons_from_js(js_text: str) -> Iterable[Any]:
    # 1) JSON.parse("...") 形式
    for m in JSON_PARSE_CALL_RE.finditer(js_text):
        raw = m.group("body")
        try:
            decoded = _decode_js_string_literal(raw)
            yield json.loads(decoded)
        except Exception:
            continue

    # 2) 文字列中にそのままの JSON 配列
    for m in QUOTED_JSON_ARRAY_RE.finditer(js_text):
        arr_text = m.group(2)
        try:
            yield json.loads(arr_text)
        except Exception:
            continue

    # 3) バランス走査（最終手段）
    for m in BALANCED_ARRAY_START_RE.finditer(js_text):
        start = m.start()
        sliced = _balanced_slice(js_text, start, "[", "]")
        if not sliced or sliced.count("{") == 0:
            continue
        if not re.search(r'"\s*[\w\-]+"\s*:', sliced):
            continue
        try:
            yield json.loads(sliced)
        except Exception:
            continue


# ────────────────────────────────────────────────────────────
# カード行生成
# ────────────────────────────────────────────────────────────
def normalize_card_obj(obj: Dict[str, Any], source_asset: str) -> CardRow:
    title = obj.get("title") or obj.get("name") or obj.get("cardName")
    member = obj.get("member") or obj.get("idol") or obj.get("character")
    rarity = obj.get("rarity") or obj.get("rar") or obj.get("type")
    ap_cost = obj.get("ap_cost") or obj.get("ap") or obj.get("cost")
    smile = obj.get("smile")
    pure = obj.get("pure")
    cool = obj.get("cool")
    mental = obj.get("mental") or obj.get("hp")
    skill_code = obj.get("skill_code") or obj.get("skill") or obj.get("skillText")
    center_skill_code = obj.get("center_skill_code") or obj.get("centerSkill")
    center_trait_code = obj.get("center_trait_code") or obj.get("centerTrait")

    def to_int(x):
        try:
            return int(x)
        except Exception:
            return None

    return CardRow(
        id=str(obj.get("id")) if obj.get("id") is not None else None,
        title=str(title) if title is not None else None,
        member=str(member) if member is not None else None,
        rarity=str(rarity) if rarity is not None else None,
        ap_cost=to_int(ap_cost),
        smile=to_int(smile),
        pure=to_int(pure),
        cool=to_int(cool),
        mental=to_int(mental),
        skill_code=str(skill_code) if skill_code is not None else None,
        center_skill_code=str(center_skill_code) if center_skill_code is not None else None,
        center_trait_code=str(center_trait_code) if center_trait_code is not None else None,
        source_asset=source_asset,
        last_seen=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )


def _is_likely_card_dict(d: Dict[str, Any]) -> bool:
    if not isinstance(d, dict):
        return False
    keys = set(d.keys())
    score = 0
    for k in ("title", "name", "cardName"):
        if k in keys:
            score += 2
    for k in ("member", "idol", "character"):
        if k in keys:
            score += 2
    for k in ("ap_cost", "ap", "cost"):
        if k in keys:
            score += 1
    for k in ("smile", "pure", "cool", "mental", "hp"):
        if k in keys:
            score += 1
    for k in ("skill_code", "skill", "skillText"):
        if k in keys:
            score += 2

    txt = (d.get("skill") or d.get("skillText") or d.get("skill_code") or "") + " " + (d.get("title") or d.get("name") or "")

    hint_hit = sum(1 for h in SKILL_DSL_HINTS if h in txt) + sum(1 for h in MEMBER_HINTS if h in txt)
    score += min(hint_hit, 3)

    return score >= 3


def collect_cards_from_json_obj(obj: Any, source_asset: str) -> List[CardRow]:
    rows: List[CardRow] = []
    if isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj):
            for c in (x for x in obj if _is_likely_card_dict(x)):
                row = normalize_card_obj(c, source_asset=source_asset)
                if row.title or row.skill_code:
                    rows.append(row)
        else:
            for v in obj:
                rows.extend(collect_cards_from_json_obj(v, source_asset))
    elif isinstance(obj, dict):
        for v in obj.values():
            rows.extend(collect_cards_from_json_obj(v, source_asset))
    return rows


# ────────────────────────────────────────────────────────────
# メイン抽出
# ────────────────────────────────────────────────────────────
def extract_all_cards(base_url: str, workers: int = 8) -> List[CardRow]:
    session = _session()
    js_urls, payload_url = discover_assets_and_payload(base_url, session=session)

    rows: List[CardRow] = []
    seen: set[Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]] = set()

    # 1) payload を最優先
    if payload_url:
        try:
            print(f"[info] fetching payload: {payload_url}", file=sys.stderr)
            payload_text = fetch(payload_url, session=session)
            payload = json.loads(payload_text)
            for row in collect_cards_from_json_obj(payload, source_asset=payload_url):
                key = (row.id, row.title, row.member, row.skill_code)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)
        except Exception as e:
            print(f"[warn] payload parse failed: {e}", file=sys.stderr)

    # payload で十分なら JS 走査スキップ（高速化）
    if rows:
        return rows

    # 2) JS 資産を並列取得 → 抽出
    fetched = fetch_many(js_urls, session=session, max_workers=workers)
    for url, js in fetched.items():
        if not js:
            continue

        any_found = False
        for obj in iter_candidate_jsons_from_js(js):
            for r in collect_cards_from_json_obj(obj, source_asset=url):
                key = (r.id, r.title, r.member, r.skill_code)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(r)
                any_found = True

        # ヒューリスティクスの最終手段（重いので未検出の場合のみ）
        if not any_found:
            for m in re.finditer(r'\[', js):
                arr = _balanced_slice(js, m.start(), "[", "]")
                if not arr or len(arr) < 200 or "{" not in arr:
                    continue
                hint_hit = sum(1 for h in SKILL_DSL_HINTS if h in arr) + sum(1 for h in MEMBER_HINTS if h in arr)
                if hint_hit == 0:
                    continue
                if not re.search(r'"\s*[\w\-]+"\s*:', arr):
                    continue
                try:
                    data = json.loads(arr)
                except Exception:
                    continue
                for r in collect_cards_from_json_obj(data, source_asset=url):
                    key = (r.id, r.title, r.member, r.skill_code)
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append(r)

    return rows


def write_csv(rows: Iterable[CardRow], out_path: str) -> None:
    rows = list(rows)
    # Excel配慮が必要な場合は utf-8-sig（BOM付き）を推奨
    with io.open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADERS)
        w.writeheader()
        for r in rows:
            d = asdict(r)
            for h in HEADERS:
                d.setdefault(h, None)
            w.writerow({k: d.get(k) for k in HEADERS})


# ────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="スクショウ計算機のカード情報をCSV出力（高速版）")
    parser.add_argument("--base-url", default="https://asmape0104.github.io/scshow-calculator/", help="スクショウ計算機のベースURL（末尾スラッシュ推奨）")
    parser.add_argument("--out", default="cards.csv", help="出力CSVパス")
    parser.add_argument("--workers", type=int, default=8, help="並列取得のワーカ数（既定: 8）")
    args = parser.parse_args()

    base_url = args.base_url
    if not base_url.endswith("/"):
        base_url += "/"

    print(f"[info] base_url = {base_url}", file=sys.stderr)
    print(f"[info] workers = {args.workers}", file=sys.stderr)

    rows = extract_all_cards(base_url, workers=args.workers)
    print(f"[info] extracted rows = {len(rows)}", file=sys.stderr)
    write_csv(rows, args.out)
    print(f"[info] written -> {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
