# -*- coding: utf-8 -*-
"""
スクショウ計算機（https://asmape0104.github.io/scshow-calculator/）から
全カード情報を抽出して CSV を出力する高速版スクリプト（v2.0, 2025-08-31）。

主なポイント:
- /<base_url>/_nuxt/ 系列の JS のみを強制的に対象化（誤ドメイン混入を防止）
- JS バンドル内の JSON（JSON.parse("...") / 配列の生埋め込み）を解析
- JS が参照する外部 *.json（絶対/相対 URL）も取得して統合
- Nuxt の _payload.json には依存しない（存在すれば参考としても解析）
- ログ強化、重複除去、例外耐性

使い方:
  python extract.py \
    --base-url "https://asmape0104.github.io/scshow-calculator/" \
    --out cards.csv \
    --workers 8
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
from datetime import datetime, timezone
from typing import Iterable, List, Dict, Any, Optional, Tuple
import urllib.parse as up
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import requests_cache

# HTTP キャッシュ（actions/cache と連携、ローカルでも有効）
requests_cache.install_cache(".requests_cache/http-cache", expire_after=86400)

# ────────────────────────────────────────────────────────────
# Nuxt / assets 抽出用の正規表現
# ────────────────────────────────────────────────────────────
# window.__NUXT__.config = { app: { baseURL:"/scshow-calculator/", buildId:"...", buildAssetsDir:"/_nuxt" }, ... }
NUXT_APP_RE = re.compile(
    r'window\.__NUXT__\.config\s*=\s*\{[^{}]*app\s*:\s*\{[^}]*'
    r'baseURL"\s*:\s*"(?P<baseURL>[^"]+)"[^}]*'
    r'buildId"\s*:\s*"(?P<buildId>[^"]+)"[^}]*'
    r'buildAssetsDir"\s*:\s*"(?P<buildAssetsDir>[^"]+)"',
    re.S,
)

# <script id="__NUXT_DATA__" data-src="/scshow-calculator/_payload.json?...">
NUXT_DATA_SRC_RE = re.compile(
    r'id="__NUXT_DATA__"[^>]*\sdata-src="(?P<src>[^"]+_payload\.json[^"]*)"', re.S
)

# <link rel="modulepreload" href="/scshow-calculator/_nuxt/xxx.js"> や <script src="...">
HREF_OR_SRC_JS_RE = re.compile(r'(?:href|src)="([^"]*?/_nuxt/[^"]+\.js)"')
ALL_JS_PATH_RE = re.compile(r'/_nuxt/[^"\'\s]+\.js')

# JS 内から外部 JSON 参照（https://.../*.json, /foo/*.json, foo/*.json）
JSON_URL_RE = re.compile(r'(?P<q>["\'])(?P<url>(?:https?://|/)[^"\']+?\.json(?:\?[^"\']*)?)\1')
REL_JSON_URL_RE = re.compile(r'(?P<q>["\'])(?P<url>(?!https?://|/)[^"\']+?\.json(?:\?[^"\']*)?)\1')

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
        "User-Agent": "card-extractor/2.0 (+https://asmape0104.github.io/scshow-calculator/)",
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
            time.sleep(0.5 * (attempt + 1))
    # 最後に例外を投げる
    raise last_exc  # type: ignore

def fetch_many(urls: List[str], session: Optional[requests.Session] = None, max_workers: int = 8) -> Dict[str, Optional[str]]:
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
    m = NUXT_APP_RE.search(html)
    if not m:
        return (None, None, None)
    return (m.group("baseURL"), m.group("buildId"), m.group("buildAssetsDir"))

def _extract_payload_url(html: str, base_url: str) -> Optional[str]:
    m = NUXT_DATA_SRC_RE.search(html)
    if not m:
        return None
    path = m.group("src")
    if path.startswith(("http://", "https://")):
        return path
    # 先頭"/"は root 相対のまま urljoin に渡す
    return up.urljoin(base_url, path)

def _resolve_asset_url(raw_path: str, base_url: str, baseURL_from_config: Optional[str]) -> str:
    if raw_path.startswith(("http://", "https://")):
        return raw_path
    # "/_nuxt/*" は baseURL があれば "/<baseURL>/_nuxt/*" に寄せる
    if raw_path.startswith("/_nuxt/") and baseURL_from_config:
        raw_path = baseURL_from_config.rstrip("/") + raw_path
    return up.urljoin(base_url, raw_path)

def discover_assets_and_payload(base_url: str, session: Optional[requests.Session] = None) -> Tuple[List[str], Optional[str], Optional[str]]:
    index_url = up.urljoin(base_url, "index.html")
    html = fetch(index_url, session=session)
    payload_url = _extract_payload_url(html, base_url)
    js_urls: set[str] = set()
    baseURL, buildId, buildAssetsDir = _extract_meta_from_index(html)

    # builds/meta/<buildId>.json からも列挙
    if buildId and buildAssetsDir:
        meta_rel = f"{buildAssetsDir.lstrip('/')}/builds/meta/{buildId}.json"
        meta_url = up.urljoin(base_url, meta_rel)
        try:
            meta_text = fetch(meta_url, session=session)
            meta = json.loads(meta_text)
            def walk(v: Any):
                if isinstance(v, str) and v.endswith(".js") and ("/_nuxt/" in v or "/__nuxt/" in v):
                    js_urls.add(_resolve_asset_url(v, base_url, baseURL))
                elif isinstance(v, list):
                    for x in v: walk(x)
                elif isinstance(v, dict):
                    for x in v.values(): walk(x)
            walk(meta)
        except Exception:
            pass

    # index.html 内の <link|script> からも拾う
    for href in HREF_OR_SRC_JS_RE.findall(html) + ALL_JS_PATH_RE.findall(html):
        js_urls.add(_resolve_asset_url(href, base_url, baseURL))

    # --- 必須: 入力 base_url を用いた強制フィルタ（誤系統の /_nuxt/ を除外）
    preferred_prefix = base_url.rstrip("/") + "/_nuxt/"
    js_urls = {u for u in js_urls if u.startswith(preferred_prefix)}

    # debug
    try:
        print(f"[debug] discovered js_urls = {len(js_urls)}", file=sys.stderr)
        for i, u in enumerate(sorted(js_urls)[:10]):
            print(f"[debug]   js[{i}]: {u}", file=sys.stderr)
    except Exception:
        pass

    return (sorted(js_urls), payload_url, baseURL)


# ────────────────────────────────────────────────────────────
# JS/JSON 抽出
# ────────────────────────────────────────────────────────────
JSON_PARSE_CALL_RE = re.compile(r'JSON\.parse\(\s*(["\'])(?P<body>(?:\\.|(?!\1).)*)\1\s*\)')
BALANCED_ARRAY_START_RE = re.compile(r'\[')

def _decode_js_string_literal(s: str) -> str:
    # JS 文字列リテラルの簡易デコード（\" や \\uXXXX を処理）
    return json.loads(f'"{s}"')

def _balanced_slice(src: str, start_idx: int, open_ch: str, close_ch: str) -> Optional[str]:
    depth = 0; i = start_idx; in_str = False; esc = False; quote = ''
    while i < len(src):
        ch = src[i]
        if in_str:
            if esc: esc = False
            elif ch == '\\': esc = True
            elif ch == quote: in_str = False
        else:
            if ch in ("'", '"'): in_str = True; quote = ch
            elif ch == open_ch: depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    return src[start_idx:i+1]
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

    # 2) バランス走査（最終手段）: JSON 配列っぽい塊を拾う
    for m in BALANCED_ARRAY_START_RE.finditer(js_text):
        start = m.start()
        sliced = _balanced_slice(js_text, start, "[", "]")
        if not sliced or sliced.count("{") == 0:
            continue
        if not re.search(r'"\s*[\w\-\: ]+"\s*:', sliced):  # "key":
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
    smile = obj.get("smile"); pure = obj.get("pure"); cool = obj.get("cool")
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
        last_seen=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

def _contains_skill_hint(v: Any) -> bool:
    try:
        s = str(v)
    except Exception:
        return False
    s_lower = s.lower()
    return any(h in s_lower for h in SKILL_DSL_HINTS)

def _looks_like_card_obj(obj: Dict[str, Any]) -> bool:
    if not obj or not isinstance(obj, dict):
        return False
    keys = set(obj.keys())
    title_like = any(k in keys for k in ("title", "name", "cardName"))
    member_like = any(k in keys for k in ("member", "idol", "character"))
    stats_like = any(k in keys for k in ("smile", "pure", "cool", "mental", "hp"))
    skill_like = any(k in keys for k in ("skill_code", "skill", "skillText", "centerSkill"))
    # 文字列中のヒント
    hint_like = any(_contains_skill_hint(v) for v in obj.values())
    member_hint = any((isinstance(v, str) and any(m in v for m in MEMBER_HINTS)) for v in obj.values())
    score = 0
    score += 2 if title_like else 0
    score += 2 if member_like else 0
    score += 1 if stats_like else 0
    score += 1 if skill_like else 0
    score += 1 if hint_like else 0
    score += 1 if member_hint else 0
    return score >= 3  # 閾値

def extract_card_rows_from_any(value: Any, source_asset: str, rows: List[CardRow]) -> None:
    if isinstance(value, dict):
        if _looks_like_card_obj(value):
            try:
                rows.append(normalize_card_obj(value, source_asset))
            except Exception:
                pass
        # 再帰
        for v in value.values():
            extract_card_rows_from_any(v, source_asset, rows)
    elif isinstance(value, list):
        # 配列が「カード辞書の配列」っぽい場合は一括処理
        dicts = [x for x in value if isinstance(x, dict)]
        if len(dicts) >= 3:
            hit = sum(1 for d in dicts if _looks_like_card_obj(d))
            if hit >= max(3, len(dicts) // 3):
                for d in dicts:
                    if _looks_like_card_obj(d):
                        try:
                            rows.append(normalize_card_obj(d, source_asset))
                        except Exception:
                            pass
                return
        # そうでなければ個別再帰
        for v in value:
            extract_card_rows_from_any(v, source_asset, rows)


# ────────────────────────────────────────────────────────────
# メイン処理
# ────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", dest="base_url", type=str,
                    default="https://asmape0104.github.io/scshow-calculator/",
                    help="対象サイトのベースURL（末尾スラッシュ可）")
    ap.add_argument("--out", dest="out_name", type=str, default="cards.csv",
                    help="出力CSVファイル名")
    ap.add_argument("--workers", dest="workers", type=int, default=8,
                    help="並列取得ワーカー数")
    args = ap.parse_args()

    base_url = args.base_url
    out_name = args.out_name
    workers = int(args.workers)

    print(f"[info] base_url = {base_url}", file=sys.stderr)
    print(f"[info] workers = {workers}", file=sys.stderr)

    sess = _session()

    # 1) JS 資産と payload の検出
    js_urls, payload_url, baseURL_from_config = discover_assets_and_payload(base_url, session=sess)

    # 2) JS を取得
    js_map = fetch_many(js_urls, session=sess, max_workers=workers)
    js_ok = sum(1 for v in js_map.values() if v is not None)
    print(f"[info] fetched js assets = {js_ok}/{len(js_map)}", file=sys.stderr)

    # 3) JS 内から外部 *.json 参照を列挙（相対は base_url で解決）
    ext_json_urls: set[str] = set()
    for u, text in js_map.items():
        if not text:
            continue
        for m in JSON_URL_RE.finditer(text):
            ext_json_urls.add(m.group("url"))
        for m in REL_JSON_URL_RE.finditer(text):
            rel = m.group("url")
            ext_json_urls.add(up.urljoin(base_url, rel))

    # 取得ドメインを base_url と同一オリジン優先に絞る（安全のため）
    base_origin = up.urlsplit(base_url)
    def same_origin(url: str) -> bool:
        p = up.urlsplit(url)
        return (p.scheme, p.netloc) == (base_origin.scheme, base_origin.netloc)

    # まず同一オリジン、余力があれば全て
    pri_urls = sorted([u for u in ext_json_urls if same_origin(u)])
    sec_urls = sorted([u for u in ext_json_urls if not same_origin(u)])
    # 外部オリジンは過剰取得しない
    if len(sec_urls) > 50:
        sec_urls = sec_urls[:50]

    print(f"[info] discovered external json = {len(ext_json_urls)} "
          f"(same-origin={len(pri_urls)}, cross-origin={len(sec_urls)})", file=sys.stderr)

    # 4) 外部 *.json を取得
    json_bodies_map: Dict[str, Optional[str]] = {}
    if pri_urls:
        json_bodies_map.update(fetch_many(pri_urls, session=sess, max_workers=workers))
    if sec_urls:
        json_bodies_map.update(fetch_many(sec_urls, session=sess, max_workers=max(2, workers // 2)))
    json_ok = sum(1 for v in json_bodies_map.values() if v is not None)
    print(f"[info] fetched external json = {json_ok}/{len(json_bodies_map)}", file=sys.stderr)

    # 5) 候補 JSON（JS中/外部/ペイロード）を統合
    candidates: List[Tuple[str, Any]] = []

    for u, text in js_map.items():
        if not text:
            continue
        for obj in iter_candidate_jsons_from_js(text):
            candidates.append((u, obj))

    for u, body in json_bodies_map.items():
        if not body:
            continue
        try:
            candidates.append((u, json.loads(body)))
        except Exception:
            pass

    if payload_url:
        try:
            body = fetch(payload_url, session=sess)
            obj = json.loads(body)
            candidates.append((payload_url, obj))
            print("[info] payload parsed OK", file=sys.stderr)
        except Exception:
            print("[warn] payload not usable (ignored)", file=sys.stderr)

    print(f"[info] candidate JSON blobs = {len(candidates)}", file=sys.stderr)

    # 6) カード行に正規化
    rows: List[CardRow] = []
    for src, obj in candidates:
        extract_card_rows_from_any(obj, src, rows)

    # 重複除去（id/title/member/rarity/ap_cost のキーで代表）
    seen: set[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[int]]] = set()
    unique_rows: List[CardRow] = []
    for r in rows:
        key = (r.id, r.title, r.member, r.rarity, r.ap_cost)
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(r)

    print(f"[info] extracted rows = {len(unique_rows)}", file=sys.stderr)

    # 7) CSV 出力
    with io.open(out_name, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        for r in unique_rows:
            writer.writerow(asdict(r))

    print(f"[info] written -> {out_name}", file=sys.stderr)


if __name__ == "__main__":
    main()
