# -*- coding: utf-8 -*-
"""
スクショウ計算機（https://asmape0104.github.io/scshow-calculator/）から
全カード情報を抽出して CSV を出力するスクリプト（v2.1, 2025-08-31）。

強化点:
- /<base_url>/_nuxt/ 系列 JS のみを強制対象化
- JSバンドル内の 'JSON ではない' オブジェクト配列を JSON へ正規化して解析
- JS が参照する外部 *.json の列挙・取得を可視化（URL & 成否をログ出力）
- .mjs も対象
- 既知フォールバックパスの少数試行（cards.json, data/cards.json 等）
- Nuxt の _payload.json に依存しない（存在時は参考として解析）

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

# HTTP キャッシュ
requests_cache.install_cache(".requests_cache/http-cache", expire_after=86400)

# ────────────────────────────────────────────────────────────
# Nuxt / assets 検出
# ────────────────────────────────────────────────────────────
NUXT_APP_RE = re.compile(
    r'window\.__NUXT__\.config\s*=\s*\{[^{}]*app\s*:\s*\{[^}]*'
    r'baseURL"\s*:\s*"(?P<baseURL>[^"]+)"[^}]*'
    r'buildId"\s*:\s*"(?P<buildId>[^"]+)"[^}]*'
    r'buildAssetsDir"\s*:\s*"(?P<buildAssetsDir>[^"]+)"',
    re.S,
)
NUXT_DATA_SRC_RE = re.compile(
    r'id="__NUXT_DATA__"[^>]*\sdata-src="(?P<src>[^"]+_payload\.json[^"]*)"', re.S
)

# JS / MJS パス抽出
HREF_OR_SRC_ASSET_RE = re.compile(r'(?:href|src)="([^"]*?/_nuxt/[^"]+\.(?:js|mjs))"')
ALL_ASSET_PATH_RE = re.compile(r'/_nuxt/[^"\'\s]+\.(?:js|mjs)')

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
        "User-Agent": "card-extractor/2.1 (+https://asmape0104.github.io/scshow-calculator/)",
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
# Nuxt assets / payload 検出
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
    return up.urljoin(base_url, path)  # 先頭 "/" は root 相対のまま解決

def _resolve_asset_url(raw_path: str, base_url: str, baseURL_from_config: Optional[str]) -> str:
    if raw_path.startswith(("http://", "https://")):
        return raw_path
    if raw_path.startswith("/_nuxt/") and baseURL_from_config:
        raw_path = baseURL_from_config.rstrip("/") + raw_path
    return up.urljoin(base_url, raw_path)

def discover_assets_and_payload(base_url: str, session: Optional[requests.Session] = None) -> Tuple[List[str], Optional[str], Optional[str]]:
    index_url = up.urljoin(base_url, "index.html")
    html = fetch(index_url, session=session)
    payload_url = _extract_payload_url(html, base_url)
    js_urls: set[str] = set()
    baseURL, buildId, buildAssetsDir = _extract_meta_from_index(html)

    # builds/meta/<buildId>.json から列挙
    if buildId and buildAssetsDir:
        meta_rel = f"{buildAssetsDir.lstrip('/')}/builds/meta/{buildId}.json"
        meta_url = up.urljoin(base_url, meta_rel)
        try:
            meta_text = fetch(meta_url, session=session)
            meta = json.loads(meta_text)
            def walk(v: Any):
                if isinstance(v, str) and (v.endswith(".js") or v.endswith(".mjs")) and ("/_nuxt/" in v or "/__nuxt/" in v):
                    js_urls.add(_resolve_asset_url(v, base_url, baseURL))
                elif isinstance(v, list):
                    for x in v: walk(x)
                elif isinstance(v, dict):
                    for x in v.values(): walk(x)
            walk(meta)
        except Exception:
            pass

    # index.html 内の <link|script> などから抽出
    for href in HREF_OR_SRC_ASSET_RE.findall(html) + ALL_ASSET_PATH_RE.findall(html):
        js_urls.add(_resolve_asset_url(href, base_url, baseURL))

    # 入力 base_url による強制フィルタ
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
# JS/JSON 抽出（JSON ではない JS オブジェクト配列にも対応）
# ────────────────────────────────────────────────────────────
JSON_PARSE_CALL_RE = re.compile(r'JSON\.parse\(\s*(["\'])(?P<body>(?:\\.|(?!\1).)*)\1\s*\)')
BALANCED_ARRAY_START_RE = re.compile(r'\[')

def _decode_js_string_literal(s: str) -> str:
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

def _js_like_to_json_text(js_text: str) -> Optional[str]:
    """
    JS オブジェクト配列っぽい文字列を JSON 文字列に正規化して返す。
    主な処理: 単引用符→二重引用符 / キーにクオート付与 / 末尾カンマ除去 / 真偽・undefined 補正
    """
    t = js_text

    # コメント類は極力無い前提だが、念のため /**/ を除去（簡易）
    t = re.sub(r'/\*.*?\*/', '', t, flags=re.S)

    # 単引用符の文字列をダブルクオートに（エスケープ考慮の簡易置換）
    # まずは安全側: キーの正規化を先に行う
    # キー: { id: , title: } → {"id": , "title": }
    t = re.sub(r'([{\[,]\s*)([A-Za-z_][A-Za-z0-9_]*)(\s*:)', r'\1"\2"\3', t)

    # true/false/null 相当のショートハンド
    t = t.replace('!0', 'true').replace('!1', 'false')
    t = re.sub(r'\bundefined\b', 'null', t)
    t = re.sub(r'\bNaN\b', 'null', t)

    # 単引用符の文字列をダブルクオートに（雑に置換すると壊れるので最小限）
    # ここでは、: '...' / [ '...' ] / , '...' , のパターンに限定して置換
    def _sq_to_dq(m: re.Match) -> str:
        inner = m.group(1)
        inner = inner.replace('\\\'', '\'').replace('"', '\\"')
        return '"' + inner + '"'
    t = re.sub(r":\s*'([^']*)'", lambda m: ':' + _sq_to_dq(m), t)
    t = re.sub(r",\s*'([^']*)'", lambda m: ',' + _sq_to_dq(m), t)
    t = re.sub(r"\[\s*'([^']*)'\s*\]", lambda m: '[' + _sq_to_dq(m) + ']', t)

    # 末尾カンマ削除
    t = re.sub(r',(\s*[}\]])', r'\1', t)

    # ダブルクオートで囲われていない数値の末尾などは JSON として許容されるため特別処理不要
    return t

def iter_candidate_jsons_from_js(js_text: str) -> Iterable[Any]:
    # 1) JSON.parse("...") 形式
    for m in JSON_PARSE_CALL_RE.finditer(js_text):
        raw = m.group("body")
        try:
            decoded = _decode_js_string_literal(raw)
            yield json.loads(decoded)
        except Exception:
            continue

    # 2) バランス走査: 配列スライスを取り出し、JSON として/JS正規化後に読む
    for m in BALANCED_ARRAY_START_RE.finditer(js_text):
        start = m.start()
        sliced = _balanced_slice(js_text, start, "[", "]")
        if not sliced or sliced.count("{") == 0:
            continue

        # まずは JSON として直接試す
        try:
            yield json.loads(sliced)
            continue
        except Exception:
            pass

        # JSON でなければ JS → JSON 正規化を試す
        if re.search(r'[{,]\s*[A-Za-z_][A-Za-z0-9_]*\s*:', sliced):  # 未クオートキーがありそう
            norm = _js_like_to_json_text(sliced)
            if norm:
                try:
                    yield json.loads(norm)
                except Exception:
                    pass

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
    hint_like = any(_contains_skill_hint(v) for v in obj.values())
    member_hint = any((isinstance(v, str) and any(m in v for m in MEMBER_HINTS)) for v in obj.values())
    score = 0
    score += 2 if title_like else 0
    score += 2 if member_like else 0
    score += 1 if stats_like else 0
    score += 1 if skill_like else 0
    score += 1 if hint_like else 0
    score += 1 if member_hint else 0
    return score >= 3

def extract_card_rows_from_any(value: Any, source_asset: str, rows: List[CardRow]) -> None:
    if isinstance(value, dict):
        if _looks_like_card_obj(value):
            try:
                rows.append(normalize_card_obj(value, source_asset))
            except Exception:
                pass
        for v in value.values():
            extract_card_rows_from_any(v, source_asset, rows)
    elif isinstance(value, list):
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
        for v in value:
            extract_card_rows_from_any(v, source_asset, rows)

# ────────────────────────────────────────────────────────────
# メイン
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

    base_url = args.base_url.rstrip("/") + "/"
    out_name = args.out_name
    workers = int(args.workers)

    print(f"[info] base_url = {base_url}", file=sys.stderr)
    print(f"[info] workers = {workers}", file=sys.stderr)

    sess = _session()

    # 1) 資産検出
    js_urls, payload_url, _ = discover_assets_and_payload(base_url, session=sess)

    # 2) JS 取得
    js_map = fetch_many(js_urls, session=sess, max_workers=workers)
    js_ok = sum(1 for v in js_map.values() if v is not None)
    print(f"[info] fetched js assets = {js_ok}/{len(js_map)}", file=sys.stderr)

    # 3) JS 内の外部 *.json を列挙（相対は base_url で解決）
    ext_json_urls: set[str] = set()
    for u, text in js_map.items():
        if not text:
            continue
        for m in JSON_URL_RE.finditer(text):
            ext_json_urls.add(m.group("url"))
        for m in REL_JSON_URL_RE.finditer(text):
