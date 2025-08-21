# ilive_feed.py
from __future__ import annotations
import io, json, csv, os, time, re
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse

import httpx
import feedparser
from dateutil import parser as dtparse
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
import html as htmllib

FEED_URL = "https://investinglive.com/feed"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
CACHE_FILE = ".ilive_feed_cache.json"
NY_TZ = ZoneInfo("America/New_York")
MAX_SUMMARY_CHARS = 220

KEYWORD_TAGS = {
    r"\b(xau|gold)\b": "#XAU",
    r"\b(silver|xag)\b": "#XAG",
    r"\b(dxy|us dollar|greenback|usd index|dollar index)\b": "#DXY",
    r"\b(eur/usd|eurusd|euro)\b": "#EURUSD",
    r"\b(usd/jpy|usdjpy|yen)\b": "#USDJPY",
    r"\b(gbp/usd|gbpusd|sterling|pound)\b": "#GBPUSD",
    r"\b(wti|brent|crude|oil)\b": "#OIL",
    r"\b(copper)\b": "#HG",
    r"\b(10[- ]?year|10y|ust10|ust 10|ten[- ]year|treasury yield|yields?)\b": "#UST10Y",
    r"\b(2[- ]?year|2y|ust2)\b": "#UST2Y",
    r"\b(cpi|ppi|pce|core pce|payrolls|nfp|unemployment)\b": "#DATA",
    r"\b(fed|powell|fomc|dot plot|hike|cut|rate decision)\b": "#Fed",
    r"\b(ecb|lagarde|boj|boe|snb|rba|boc)\b": "#CB",
    r"\b(risk[- ]?on|risk[- ]?off|sentiment)\b": "#RISK",
}

@dataclass
class Item:
    title: str
    link: str
    published_ny: Optional[str]
    tags: List[str]
    summary: str

def _load_cache() -> Dict[str, Any]:
    if os.path.exists(CACHE_FILE):
        try:
            import json
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_cache(cache: Dict[str, Any]) -> None:
    try:
        import json
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f)
    except Exception:
        pass

def polite_fetch(url: str, max_retries: int = 5, timeout: float = 15.0) -> Optional[str]:
    cache = _load_cache()
    headers = {
        "User-Agent": UA,
        "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    if et := cache.get("etag"):
        headers["If-None-Match"] = et
    if lm := cache.get("last_modified"):
        headers["If-Modified-Since"] = lm

    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            with httpx.Client(http2=True, headers=headers, follow_redirects=True, timeout=timeout) as client:
                r = client.get(url)
            if r.status_code == 304:
                return None
            if r.status_code == 200:
                new_cache = {
                    "etag": r.headers.get("ETag") or cache.get("etag"),
                    "last_modified": r.headers.get("Last-Modified") or cache.get("last_modified"),
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                }
                _save_cache(new_cache)
                return r.text
            if r.status_code in (429, 503):
                time.sleep(backoff + (0.1 * attempt))
                backoff = min(backoff * 2, 30)
                continue
            r.raise_for_status()
        except httpx.HTTPError:
            if attempt == max_retries:
                raise
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    return None

def _to_ny(dt: datetime) -> datetime:
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(NY_TZ)

def _coerce_dt(s: str | None, fallback_struct=None) -> Optional[datetime]:
    if not s and not fallback_struct:
        return None
    try:
        if s:
            return dtparse.parse(s)
    except Exception:
        pass
    if fallback_struct:
        try:
            return datetime(*fallback_struct[:6], tzinfo=timezone.utc)
        except Exception:
            return None
    return None

def _extract_tags(text: str) -> List[str]:
    tags = []
    lower = text.lower()
    for pat, tag in KEYWORD_TAGS.items():
        if re.search(pat, lower):
            tags.append(tag)
    seen, ordered = set(), []
    for t in tags:
        if t not in seen:
            seen.add(t)
            ordered.append(t)
    return ordered

def _html_to_text(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for ul in soup.find_all(["ul", "ol"]):
        for li in ul.find_all("li"):
            li.insert_before("\n• ")
        ul.unwrap()
    text = soup.get_text(separator=" ", strip=True)
    text = htmllib.unescape(text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\s{2,}", " ", text).strip()
    if len(text) > MAX_SUMMARY_CHARS:
        text = text[:MAX_SUMMARY_CHARS].rstrip() + "…"
    return text

def _section_tag_from_url(link: str) -> str | None:
    try:
        path = urlparse(link).path.strip("/")
        first = (path.split("/", 1) + [""])[0]
        if first:
            return f"#{first.replace('-', '_')}".upper()
    except Exception:
        pass
    return None

def parse_feed(xml_text: str) -> List[Item]:
    fp = feedparser.parse(xml_text)
    items: List[Item] = []
    for e in fp.entries:
        dt_raw = e.get("published") or e.get("updated") or ""
        dt_struct = e.get("published_parsed") or e.get("updated_parsed")
        dt = _coerce_dt(dt_raw, dt_struct)
        dt_ny = _to_ny(dt).strftime("%Y-%m-%d %H:%M") if dt else None

        title = e.get("title", "").strip()
        summary = _html_to_text(e.get("summary", "") or "")
        link = e.get("link", "").strip()

        all_text = f"{title} {summary}"
        tags = _extract_tags(all_text)
        sec = _section_tag_from_url(link)
        if sec and sec not in tags:
            tags.insert(0, sec)

        items.append(Item(title=title, link=link, published_ny=dt_ny, tags=tags, summary=summary))
    items.sort(key=lambda x: x.published_ny or "", reverse=True)
    return items

def filter_items(items: List[Item], hours: Optional[int], limit: Optional[int]) -> List[Item]:
    if hours:
        cutoff = datetime.now(NY_TZ) - timedelta(hours=hours)
        items = [
            it for it in items
            if (it.published_ny and dtparse.parse(it.published_ny).replace(tzinfo=NY_TZ) >= cutoff)
        ]
    if limit:
        items = items[:limit]
    return items

def render_markdown(items: List[Item]) -> str:
    out = io.StringIO()
    current_date = None
    print("# InvestingLive Feed Digest (NY time)\n", file=out)
    for it in items:
        date_part = it.published_ny.split(" ")[0] if it.published_ny else "Unknown"
        if date_part != current_date:
            current_date = date_part
            print(f"\n## {current_date}", file=out)
        tag_str = " ".join(it.tags) if it.tags else ""
        time_part = it.published_ny.split(" ")[1] if it.published_ny else "--:--"
        title = it.title.replace("\n", " ").strip()
        print(f"- **{time_part}** — [{title}]({it.link})  {tag_str}", file=out)
        if it.summary:
            s = it.summary.replace("\n• ", "\n    • ")
            print(f"  - {s}", file=out)
    return out.getvalue().strip()

def render_json(items: List[Item]) -> str:
    return json.dumps([asdict(it) for it in items], ensure_ascii=False, indent=2)

def render_csv(items: List[Item]) -> str:
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["published_ny", "title", "tags", "link", "summary"])
    for it in items:
        writer.writerow([
            it.published_ny or "",
            it.title,
            " ".join(it.tags),
            it.link,
            it.summary.replace("\n", " ").strip(),
        ])
    return out.getvalue()

def get_digest(url: str = FEED_URL, hours: int | None = None, limit: int = 50) -> List[Item]:
    xml = polite_fetch(url)
    if xml is None:
        # No change; return empty list (caller can decide how to handle)
        return []
    items = parse_feed(xml)
    return filter_items(items, hours, limit)
