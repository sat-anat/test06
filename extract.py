# -*- coding: utf-8 -*-
"""
スクショウ計算機（https://asmape0104.github.io/scshow-calculator/）から
全カード情報を抽出して CSV を出力する高速版スクリプト（v1.5）。

主な改良点:
- JS 資産URLを baseURL (/scshow-calculator/) に正規化し、ルート直下 /_nuxt/* を除外
- JS 内から外部 JSON の参照（.json 文字列）を検出して取得・解析
- ログ強化（列挙/取得本数、検出 JSON 数）

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
from typing import Iterable, List, Dict, Any, Optional, Tuple
import urllib.parse as up
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import requests_cache

requests_cache.install_cache(".requests_cache/http-cache", expire_after=86400)

# ────────────────────────────────────────────────────────────
# Nuxt / assets 抽出用の正規表現
# ────────────────────────────────────────────────────────────
NUXT_APP_RE = re.compile(
    r'window\.__NUXT__\.config\s*=\s*\{[^{}]*app\s*:\s*\{[^}]*'
    r'baseURL"\s*:\s*"(?P<baseURL>[^"]+)"[^}]*'
    r'buildId"\s*:\s*"(?P<buildId>[^"]+)"[^}]*'
    r'buildAssetsDir"\s*:\s*"(?P<buildAssetsDir>[^"]+)"',
    re.S,
)
NUXT_DATA_SRC_RE = re.compile(
    r'id="__NUXT_DATA__"[^>]*\sdata-src="(?P<src>[^"]+_payload\.json[^"]*)"',
    re.S,
)
MODULE_HREF_RE = re.compile(r'href="([^"]*/_nuxt/[^"]+\.js)"')
ALL_JS_PATH_RE = re.compile(r'/_nuxt/[^"\'\s]+\.js')

# JS 内から外部 JSON らしき参照を拾う（簡易）
#   "https://.../something.json?..." / '/scshow-calculator/data.json' / 'data/cards.json'
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
        "User-Agent": "card-extractor/1.5 (+https://asmape0104.github.io/scshow-calculator/)",
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
    return up.urljoin(base_url, path)  # 先頭"/"はそのまま渡してroot相対解決

def _resolve_asset_url(raw_path: str, base_url: str, baseURL_from_config: Optional[str]) -> str:
    if raw_path.startswith(("http://", "https://")):
        return raw_path
    # /_nuxt/* は baseURL があれば /<baseURL>/_nuxt/* に寄せる
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
            def walk(v):
                if isinstance(v, str) and v.endswith(".js") and ("/_nuxt/" in v or "/__nuxt/" in v):
                    js_urls.add(_resolve_asset_url(v, base_url, baseURL))
                elif isinstance(v, list):
                    for x in v: walk(x)
                elif isinstance(v, dict):
                    for x in v.values(): walk(x)
            walk(meta)
        except Exception:
            pass

    for href in MODULE_HREF_RE.findall(html) + ALL_JS_PATH_RE.findall(html):
        js_urls.add(_resolve_asset_url(href, base_url, baseURL))

    # baseURL がある場合は /_nuxt/ 直下を除外し、/<baseURL>/_nuxt/ を優先
    if baseURL:
        canonical_prefix = baseURL.rstrip("/") + "/_nuxt/"
        js_urls = {u for u in js_urls if ("/_nuxt/" in u and canonical_prefix in u)}

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
JSON_PARSE_CALL_RE = re.compile(r'JSON\.parse\(\s*([\'"])(?P<body>(?:\\.|(?!\1).)*)\1\s*\)')
QUOTED_JSON_ARRAY_RE = re.compile(r'([\'"])(\[\s*\{.*?\}\s*\])\1', re.S)
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
                if depth == 0: return src[start_idx:i+1]
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
        if not sliced or sliced.count("{") == 0: continue
        if not re.search(r'"\s*[\w\-]+"\s*:', sliced): continue
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
        try: return int(x)
        except Exception: return None

    return CardRow(
        id=str(obj.get("id")) if obj.get("id") is not None else None,
        title=str(title) if title is not None else None,
