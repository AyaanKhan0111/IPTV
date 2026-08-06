#!/usr/bin/env python3
"""Build a verified IPTV playlist from public sources.

Pipeline: fetch sources -> normalise -> classify -> dedupe -> verify -> export.

Robustness features:
  * Every source fetch is retried, and a source that fails is skipped rather
    than aborting the run.
  * Streams get more than one verification attempt, and a stream that fails is
    only dropped after `grace_failures` consecutive bad runs (tracked in
    reports/health.json) so a transient upstream blip does not gut the playlist.
  * A safety gate refuses to overwrite the playlist if the channel count
    collapses, which would otherwise ship an empty playlist to users.
  * Output files are written atomically.
"""

import argparse
import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

import aiohttp

CONFIG_PATH = "config.json"
PLAYLIST_PATH = "playlist.m3u"
REPORTS_DIR = "reports"
HEALTH_PATH = os.path.join(REPORTS_DIR, "health.json")
STATS_PATH = os.path.join(REPORTS_DIR, "stats.json")
DEAD_PATH = os.path.join(REPORTS_DIR, "dead_channels.txt")
SOURCES_PATH = os.path.join(REPORTS_DIR, "sources.json")
REGION_LOCKED_PATH = os.path.join(REPORTS_DIR, "region_locked.txt")

IPTV_ORG_API = "https://iptv-org.github.io/api"

DEFAULT_USER_AGENT = "VLC/3.0.20 LibVLC/3.0.20"

QUALITY_RANK = {
    "2160p": 0, "4k": 0, "1440p": 1, "1080p": 2, "1080i": 3,
    "720p": 4, "576p": 5, "480p": 6, "406p": 7, "360p": 8, "240p": 9,
}

# ISO codes we are willing to keep something from. Anything else is dropped
# unless a channel_groups rule claims it.
KNOWN_COUNTRIES = {
    "pk", "in", "us", "uk", "gb", "ca", "au", "nz", "za", "ie",
    "ae", "sa", "qa", "kw", "bd", "lk", "np", "mv", "af",
}

# Tokens that identify a country inside a group-title. Deliberately spelled out:
# matching bare two-letter codes against group titles is how "Movies" used to be
# classified as Ireland.
GROUP_COUNTRY_TOKENS = [
    ("pakistan", "pk"), ("pakistani", "pk"), ("urdu", "pk"),
    ("india", "in"), ("indian", "in"), ("hindi", "in"), ("punjabi", "in"),
    ("tamil", "in"), ("telugu", "in"), ("malayalam", "in"), ("kannada", "in"),
    ("bengali", "in"), ("marathi", "in"), ("gujarati", "in"), ("bhojpuri", "in"),
    ("bangladesh", "bd"), ("bangla", "bd"),
    ("sri lanka", "lk"), ("sinhala", "lk"),
    ("nepal", "np"), ("nepali", "np"),
    ("united states", "us"), ("usa", "us"), ("american", "us"),
    ("united kingdom", "uk"), ("great britain", "uk"), ("british", "uk"), ("england", "uk"),
    ("canada", "ca"), ("canadian", "ca"),
    ("australia", "au"), ("australian", "au"),
    ("new zealand", "nz"),
    ("south africa", "za"),
    ("ireland", "ie"), ("irish", "ie"),
    ("united arab emirates", "ae"), ("dubai", "ae"),
    ("saudi arabia", "sa"),
]

PK_NAME_PATTERNS = [
    r"\bary\b", r"\bgeo\b", r"\bhum\b", r"\bptv\b", r"\bten\s*sport",
    r"\bexpress\s*(news|entertainment)", r"\bdunya\b", r"\bsamaa\b", r"\b92\s*news\b",
    r"\bgnn\b", r"\bbol\s*news\b", r"\bneo\s*tv\b", r"\bsuno\s*tv\b", r"\bdawn\s*news\b",
    r"\bpakistan", r"\bpublic\s*news\b", r"\babb\s*takk\b", r"\bcapital\s*tv\b",
]

IN_NAME_PATTERNS = [
    r"\bdd\s", r"\bdoordarshan\b", r"\bsony\b", r"\bcolors\b", r"\bstar\b", r"\bzee\b",
    r"\bpogo\b", r"\bsun\s*tv\b", r"\betv\b", r"\bgemini\b", r"\bsurya\b", r"\budaya\b",
    r"\basianet\b", r"\bmazhavil\b", r"\bkairali\b", r"\bamrita\b", r"\bflowers\s*tv\b",
    r"\baaj\s*tak\b", r"\babp\b", r"\bndtv\b", r"\brepublic\b", r"\bindia\s*tv\b",
    r"\bdangal\b", r"\bshemaroo\b", r"\bb4u\b", r"\bgoldmines\b", r"\benterr10\b",
]

DRM_MARKERS = ("license_type", "widevine", "playready", "clearkey", "#kodiprop")


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

def load_config(path=CONFIG_PATH):
    defaults = {
        "countries": ["pk", "in"],
        "timeout": 10,
        "source_timeout": 60,
        "source_retries": 3,
        "stream_attempts": 2,
        "max_threads": 60,
        "max_per_host": 4,
        "remove_duplicates": True,
        "sort_channels": True,
        "drop_drm_channels": True,
        "max_streams_per_channel": 3,
        "grace_failures": 2,
        "keep_geo_blocked": True,
        "geo_blocked_reasons": ["http 403", "http 451", "http 401"],
        "geo_blocked_groups": [],
        "safety": {"min_channels": 300, "max_drop_ratio": 0.5},
        "favorites": [],
        "sources": {"iptv_org_api": True, "m3u": {}},
        "exclude_patterns": [],
        "channel_groups": [],
        "categories": ["News", "Sports", "Entertainment", "Religious", "Music", "Kids", "Documentary", "Movies"],
        "fallback_countries": ["pk", "in"],
        "fallback_country_names": {"pk": "Pakistani", "in": "Indian"},
        "fallback_categories_for_foreign": ["Movies", "Entertainment", "Kids", "Documentary"],
        "group_order": [],
        "outputs": {},
        "epg_urls": [],
        "verify_with_ffprobe": False,
    }
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                user = json.load(fh)
            for key, value in defaults.items():
                user.setdefault(key, value)
            return user
        except Exception as exc:
            print(f"[config] {path} unreadable ({exc}); falling back to defaults.")
    return defaults


def compile_group_rules(config):
    """Pre-compile the channel_groups rules into (rule, matchers, excluders)."""
    rules = []
    for raw in config.get("channel_groups", []):
        matchers = [re.compile(p, re.IGNORECASE) for p in raw.get("match", [])]
        excluders = [re.compile(p, re.IGNORECASE) for p in raw.get("exclude", [])]
        countries = {c.lower() for c in raw.get("countries", [])}
        rules.append({
            "group": raw["group"],
            "category": raw.get("category", "General"),
            "match": matchers,
            "exclude": excluders,
            "countries": countries,
        })
    return rules


# --------------------------------------------------------------------------- #
# Fetching
# --------------------------------------------------------------------------- #

async def fetch_text(session, url, name, timeout, retries):
    """GET a URL with retries. Returns ("", error) on total failure."""
    last_error = ""
    for attempt in range(1, retries + 1):
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=timeout),
                headers={"User-Agent": "Mozilla/5.0 (compatible; iptv-builder)"},
                ssl=False,
            ) as response:
                if response.status != 200:
                    last_error = f"HTTP {response.status}"
                else:
                    text = await response.text(errors="ignore")
                    print(f"[fetch] {name}: {len(text):,} chars")
                    return text, ""
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        if attempt < retries:
            await asyncio.sleep(2 * attempt)
    print(f"[fetch] {name}: FAILED after {retries} attempts ({last_error})")
    return "", last_error


async def fetch_json(session, url, name, timeout, retries):
    text, error = await fetch_text(session, url, name, timeout, retries)
    if not text:
        return None, error
    try:
        return json.loads(text), ""
    except Exception as exc:
        return None, f"invalid JSON: {exc}"


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #

ATTR_RE = re.compile(r'([\w-]+)="([^"]*)"')


def parse_extinf(line):
    comma = line.rfind(",")
    if comma != -1:
        display = line[comma + 1:].strip()
        attrs_part = line[:comma]
    else:
        display, attrs_part = "", line
    attrs = dict(ATTR_RE.findall(attrs_part))
    return {
        "tvg_id": attrs.get("tvg-id", ""),
        "tvg_name": attrs.get("tvg-name", display),
        "logo": attrs.get("tvg-logo", ""),
        "group_title": attrs.get("group-title", ""),
        "name": display,
    }


def parse_m3u(text, source_name):
    """Parse an M3U playlist into channel dicts."""
    channels = []
    info = None
    opts = []
    extgrp = ""
    drm = False

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#EXTM3U"):
            continue
        if line.startswith("#EXTINF"):
            info = parse_extinf(line)
            opts, extgrp, drm = [], "", False
        elif line.startswith("#EXTGRP:"):
            extgrp = line.split(":", 1)[1].strip()
        elif line.startswith("#EXTVLCOPT"):
            opts.append(line)
        elif line.startswith("#KODIPROP") or line.startswith("#EXTHTTP"):
            if any(marker in line.lower() for marker in DRM_MARKERS):
                drm = True
            opts.append(line)
        elif line.startswith("#"):
            continue
        elif line.startswith(("http://", "https://")):
            if info:
                if extgrp and not info["group_title"]:
                    info["group_title"] = extgrp
                info.update({
                    "url": line,
                    "opts": opts,
                    "source": source_name,
                    "country": "",
                    "api_categories": [],
                    "quality": "",
                    "drm": drm,
                })
                channels.append(info)
            info, opts, extgrp, drm = None, [], "", False

    return channels


def build_from_api(channels_json, streams_json, logos_json):
    """Turn the iptv-org API dumps into channel dicts."""
    meta = {}
    for entry in channels_json:
        if entry.get("is_nsfw") or entry.get("closed"):
            continue
        meta[entry["id"]] = entry

    logos = {}
    for logo in logos_json or []:
        channel = logo.get("channel")
        if not channel or channel in logos:
            continue
        if logo.get("in_use") is False:
            continue
        logos[channel] = logo.get("url", "")

    results = []
    for stream in streams_json:
        url = stream.get("url")
        if not url or not url.startswith(("http://", "https://")):
            continue

        channel_id = stream.get("channel") or ""
        entry = meta.get(channel_id)
        if channel_id and not entry:
            # Channel is NSFW/closed, or unknown - skip rather than guess.
            continue

        name = entry["name"] if entry else (stream.get("title") or "")
        if not name:
            continue

        opts = []
        if stream.get("user_agent"):
            opts.append(f'#EXTVLCOPT:http-user-agent={stream["user_agent"]}')
        if stream.get("referrer"):
            opts.append(f'#EXTVLCOPT:http-referrer={stream["referrer"]}')

        results.append({
            "tvg_id": channel_id,
            "tvg_name": name,
            "name": name,
            "logo": logos.get(channel_id, ""),
            "group_title": "",
            "url": url,
            "opts": opts,
            "source": "iptv-org",
            "country": (entry["country"] if entry else "").lower(),
            "api_categories": [c.lower() for c in (entry.get("categories") or [])] if entry else [],
            "quality": (stream.get("quality") or "").lower(),
            "drm": False,
        })
    return results


# --------------------------------------------------------------------------- #
# Classification
# --------------------------------------------------------------------------- #

def guess_country(channel):
    """Best-effort ISO country code for a channel. Empty string if unknown."""
    if channel.get("country"):
        code = channel["country"].lower()
        return "uk" if code == "gb" else code

    # tvg-id suffix, e.g. "StarPlus.in" or "BBCOne.uk@London"
    match = re.search(r"\.([a-z]{2})(?:@|\b)", channel.get("tvg_id", ""), re.IGNORECASE)
    if match:
        code = match.group(1).lower()
        if code in KNOWN_COUNTRIES:
            return "uk" if code == "gb" else code

    source = channel.get("source", "")
    if source in ("hindi_punjabi", "india_iptv"):
        return "in"
    if source == "nz_mjh":
        return "nz"

    haystack = f"{channel.get('group_title', '')}".lower()
    haystack = re.sub(r"\[?(non[- ])?geo[- ]blocked\]?", " ", haystack)
    for token, code in GROUP_COUNTRY_TOKENS:
        if re.search(rf"\b{re.escape(token)}\b", haystack):
            return code

    name = channel.get("name", "").lower()
    name = re.sub(r"\[?(non[- ])?geo[- ]blocked\]?", " ", name)
    if any(re.search(p, name) for p in PK_NAME_PATTERNS):
        return "pk"
    if any(re.search(p, name) for p in IN_NAME_PATTERNS):
        return "in"
    return ""


def guess_category(channel, config):
    """Map a channel onto one of the configured categories."""
    api_categories = channel.get("api_categories", [])
    api_map = {
        "news": "News", "business": "News", "weather": "News",
        "sports": "Sports", "outdoor": "Sports",
        "movies": "Movies", "classic": "Movies",
        "kids": "Kids", "animation": "Kids", "family": "Kids",
        "documentary": "Documentary", "science": "Documentary",
        "history": "Documentary", "nature": "Documentary", "travel": "Documentary",
        "music": "Music", "religious": "Religious",
        "entertainment": "Entertainment", "comedy": "Entertainment",
        "series": "Entertainment", "culture": "Entertainment",
    }
    for api_category in api_categories:
        if api_category in api_map:
            return api_map[api_category]

    haystack = f"{channel.get('group_title', '')} {channel.get('name', '')}".lower()
    for category in config["categories"]:
        if re.search(rf"\b{category.lower()}\b", haystack):
            return category
    if re.search(r"\b(movie|film|cinema|cine)\b", haystack):
        return "Movies"
    if re.search(r"\b(kid|cartoon|animation|toon)\b", haystack):
        return "Kids"
    if re.search(r"\b(religio|spiritual|islam|christian|hindu|sikh|quran|bhakti)", haystack):
        return "Religious"
    if re.search(r"\b(news|business|weather)\b", haystack):
        return "News"
    if re.search(r"\bsport", haystack):
        return "Sports"
    if re.search(r"\b(documentary|science|history|nature|wildlife)\b", haystack):
        return "Documentary"
    if re.search(r"\b(music|song|hits)\b", haystack):
        return "Music"
    if re.search(r"\b(entertainment|comedy|drama|series|show)\b", haystack):
        return "Entertainment"
    return "General"


def classify(channel, config, rules):
    """Assign final_group / category. Returns False if the channel is dropped."""
    haystack = f"{channel.get('name', '')} {channel.get('tvg_id', '')} {channel.get('tvg_name', '')}"
    country = guess_country(channel)
    channel["country_code"] = country

    # 1. Explicit channel_groups rules win.
    for rule in rules:
        if rule["countries"] and country not in rule["countries"]:
            continue
        if not any(pattern.search(haystack) for pattern in rule["match"]):
            continue
        if any(pattern.search(haystack) for pattern in rule["exclude"]):
            continue
        channel["final_group"] = rule["group"]
        channel["category"] = rule["category"]
        return True

    category = guess_category(channel, config)
    channel["category"] = category

    # 2. Home countries: keep everything.
    country_names = config["fallback_country_names"]
    if country in config["countries"] or country in country_names:
        label = country_names.get(country, country.upper())
        if country == "in" and category in ("Movies", "Entertainment", "General"):
            channel["category"] = "Entertainment"
            channel["final_group"] = "Indian Entertainment"
        else:
            channel["final_group"] = f"{label} {category}"
        return True

    # 3. Other countries: only the categories worth carrying.
    if country in config["fallback_countries"] and category in config["fallback_categories_for_foreign"]:
        channel["final_group"] = f"{country.upper()} {category}"
        return True

    return False


# --------------------------------------------------------------------------- #
# Dedupe
# --------------------------------------------------------------------------- #

def normalise_name(name):
    name = name.lower()
    name = re.sub(r"\(\s*\d{3,4}[pi]\s*\)", " ", name)          # (1080p)
    name = re.sub(r"\[[^\]]*\]", " ", name)                      # [Not 24/7]
    name = re.sub(r"\b(hd|fhd|uhd|sd|4k|tv)\b", " ", name)
    name = re.sub(r"[^a-z0-9]+", "", name)
    return name


def dedupe(channels, config):
    """Drop duplicate URLs, then cap how many streams each channel keeps."""
    seen_urls = set()
    unique = []
    for channel in channels:
        url = channel["url"]
        if config["remove_duplicates"]:
            key = url.split("?")[0].rstrip("/")
            if key in seen_urls:
                continue
            seen_urls.add(key)
        unique.append(channel)

    limit = config.get("max_streams_per_channel", 0)
    if limit <= 0:
        return unique

    def preference(channel):
        return (
            QUALITY_RANK.get(channel.get("quality", ""), 5),
            0 if channel["url"].startswith("https://") else 1,
            0 if channel.get("source") == "iptv-org" else 1,
        )

    buckets = defaultdict(list)
    for channel in unique:
        identity = channel.get("tvg_id", "").lower() or normalise_name(channel["name"])
        buckets[(channel["final_group"], identity)].append(channel)

    capped = []
    for group in buckets.values():
        group.sort(key=preference)
        capped.extend(group[:limit])
    return capped


# --------------------------------------------------------------------------- #
# Verification
# --------------------------------------------------------------------------- #

def headers_for(channel):
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    for opt in channel.get("opts", []):
        if "http-user-agent=" in opt:
            headers["User-Agent"] = opt.split("http-user-agent=", 1)[1].strip()
        elif "http-referrer=" in opt:
            headers["Referer"] = opt.split("http-referrer=", 1)[1].strip()
    return headers


def check_with_ffprobe(url, timeout_sec, headers):
    if not shutil.which("ffprobe"):
        return None
    cmd = ["ffprobe", "-v", "error"]
    if headers:
        cmd += ["-headers", "".join(f"{k}: {v}\r\n" for k, v in headers.items())]
    cmd += [
        "-show_entries", "format=format_name",
        "-of", "default=noprint_wrappers=1",
        "-rw_timeout", str(int(timeout_sec * 1_000_000)),
        url,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec + 5)
        return result.returncode == 0
    except Exception:
        return False


async def probe_stream(channel, session, timeout, use_ffprobe):
    """Return (ok, reason). `reason` is only meaningful when ok is False."""
    try:
        async with session.get(
            channel["url"],
            headers=headers_for(channel),
            timeout=aiohttp.ClientTimeout(total=timeout, sock_connect=min(timeout, 6)),
            ssl=False,
            allow_redirects=True,
        ) as response:
            if response.status != 200:
                return False, f"http {response.status}"

            chunk = await response.content.read(2048)
            if not chunk:
                return False, "empty body"

            body = chunk.decode("utf-8", errors="ignore").lstrip()
            lowered = body[:200].lower()
            if lowered.startswith(("<!doctype", "<html", "<?xml", "{")):
                return False, "html/portal page"

            content_type = response.headers.get("Content-Type", "").lower()
            looks_hls = (
                ".m3u8" in channel["url"].lower()
                or "mpegurl" in content_type
                or body.startswith("#EXTM3U")
            )
            if looks_hls:
                if "#EXTM3U" not in body:
                    return False, "not an m3u8"
                # A manifest with neither variants nor segments is a dead shell.
                if not re.search(r"#EXT-X-(STREAM-INF|MEDIA|BYTERANGE)|#EXTINF|\.ts|\.m4s|\.mp4|\.aac", body, re.I):
                    if len(chunk) >= 2048:
                        pass  # truncated read, give it the benefit of the doubt
                    else:
                        return False, "empty manifest"

            if use_ffprobe:
                ok = await asyncio.to_thread(check_with_ffprobe, channel["url"], timeout, headers_for(channel))
                if ok is False:
                    return False, "ffprobe rejected"

            return True, ""
    except asyncio.TimeoutError:
        return False, "timeout"
    except Exception as exc:
        return False, type(exc).__name__


async def verify_channels(channels, config):
    """Probe every channel, retrying failures once before declaring them dead."""
    timeout = config["timeout"]
    attempts = max(1, config.get("stream_attempts", 2))
    use_ffprobe = config.get("verify_with_ffprobe", False)

    concurrency = config["max_threads"]
    batch_size = max(concurrency * 5, config.get("batch_size", 400))

    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=config.get("max_per_host", 4),
        ttl_dns_cache=300,
        ssl=False,
        # We read 2 KB and walk away, so pooled keep-alive connections are pure
        # cost: they pin file descriptors and blow the selector limit on Windows.
        force_close=True,
        enable_cleanup_closed=True,
    )

    alive, dead = [], {}
    pending = channels

    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(concurrency)

        async def guarded(channel):
            async with semaphore:
                ok, reason = await probe_stream(channel, session, timeout, use_ffprobe)
                return channel, ok, reason

        for attempt in range(1, attempts + 1):
            if not pending:
                break
            total = len(pending)
            print(f"[verify] pass {attempt}/{attempts}: probing {total:,} streams "
                  f"(concurrency {concurrency}, {config.get('max_per_host', 4)}/host)")
            retry = []
            done = 0
            # Batched so the number of live socket handles stays bounded rather
            # than growing with the size of the playlist.
            for start in range(0, total, batch_size):
                batch = pending[start:start + batch_size]
                for future in asyncio.as_completed([guarded(channel) for channel in batch]):
                    channel, ok, reason = await future
                    done += 1
                    if ok:
                        alive.append(channel)
                        dead.pop(channel["url"], None)
                    else:
                        dead[channel["url"]] = reason
                        retry.append(channel)
                    if done % 250 == 0 or done == total:
                        print(f"[verify]   {done:,}/{total:,} - {len(alive):,} alive")
            pending = retry
            if pending and attempt < attempts:
                await asyncio.sleep(3)

    return alive, dead


# --------------------------------------------------------------------------- #
# Health history
# --------------------------------------------------------------------------- #

def split_geo_blocked(dead_channels, health, config):
    """Separate streams that are merely region-locked from genuinely dead ones.

    Verification runs from a US GitHub runner, so a channel served from inside
    Pakistan or India either answers 403/451 or refuses the connection outright
    there, while playing fine at home. Those are unverifiable rather than dead.

    A stream that has never once been reachable is still dropped eventually, so
    the playlist does not silently accumulate rot.
    """
    if not config.get("keep_geo_blocked", True):
        return [], dead_channels

    reasons = {r.lower() for r in config.get("geo_blocked_reasons", [])}
    patterns = [re.compile(p) for p in config.get("geo_blocked_groups", [])]
    if not reasons or not patterns:
        return [], dead_channels

    max_fails = config.get("unverifiable_max_fails", 30)
    geo_blocked, really_dead = [], []
    for channel, reason in dead_channels:
        group = channel.get("final_group", "")
        fails = int(health.get(channel["url"], {}).get("fails", 0))
        if (reason.lower() in reasons
                and any(p.search(group) for p in patterns)
                and fails < max_fails):
            channel["geo_blocked"] = reason
            geo_blocked.append(channel)
        else:
            really_dead.append((channel, reason))
    return geo_blocked, really_dead


def load_health():
    if not os.path.exists(HEALTH_PATH):
        return {}
    try:
        with open(HEALTH_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def apply_grace(alive, geo_blocked, dead_channels, health, grace):
    """Keep recently-good streams that failed this run, up to `grace` failures.

    Twice-daily runs mean a single bad upstream minute would otherwise evict a
    channel that works fine an hour later.
    """
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    new_health = {}
    revived = []

    alive_urls = {channel["url"] for channel in alive}
    for channel in alive:
        new_health[channel["url"]] = {"fails": 0, "last_ok": now, "name": channel["name"]}

    for channel in geo_blocked:
        alive_urls.add(channel["url"])
        previous = health.get(channel["url"], {})
        new_health[channel["url"]] = {
            "fails": int(previous.get("fails", 0)) + 1,
            "last_ok": previous.get("last_ok", ""),
            "name": channel["name"],
            "geo_blocked": channel.get("geo_blocked", ""),
        }

    for channel, reason in dead_channels:
        if channel["url"] in alive_urls:
            continue
        previous = health.get(channel["url"], {})
        fails = int(previous.get("fails", 0)) + 1
        record = {
            "fails": fails,
            "last_ok": previous.get("last_ok", ""),
            "name": channel["name"],
            "reason": reason,
        }
        new_health[channel["url"]] = record
        if fails <= grace and previous.get("last_ok"):
            channel["grace"] = fails
            revived.append(channel)

    return revived, new_health


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #

def group_sorter(config):
    order = {group: index for index, group in enumerate(config.get("group_order", []))}
    favorites = [f.lower() for f in config.get("favorites", [])]

    def key(channel):
        group = channel.get("final_group", "General")
        group_index = order.get(group, len(order))
        name = channel["name"].lower()
        is_favorite = 0 if any(f in name for f in favorites) else 1
        return (group_index, group, is_favorite, name)

    return key


def render_display_name(channel):
    name = channel["name"]
    quality = channel.get("quality", "")
    if quality and quality not in name.lower():
        name = f"{name} ({quality})"
    return name


def write_atomic(path, text):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
    os.replace(tmp, path)


def render_m3u(channels, config):
    # A single comma-separated x-tvg-url; repeating the attribute is invalid and
    # players only read the first one.
    epg_urls = config.get("epg_urls", [])
    header = "#EXTM3U"
    if epg_urls:
        joined = ",".join(epg_urls)
        header += f' x-tvg-url="{joined}" url-tvg="{joined}"'
    lines = [header]
    for channel in channels:
        lines.append(
            f'#EXTINF:-1 tvg-id="{channel["tvg_id"]}" tvg-name="{channel["tvg_name"]}" '
            f'tvg-logo="{channel["logo"]}" group-title="{channel.get("final_group", "General")}",'
            f'{render_display_name(channel)}'
        )
        lines.extend(channel.get("opts", []))
        lines.append(channel["url"])
    return "\n".join(lines) + "\n"


def export(channels, config):
    write_atomic(PLAYLIST_PATH, render_m3u(channels, config))
    print(f"[export] {PLAYLIST_PATH}: {len(channels):,} channels")

    for path, patterns in config.get("outputs", {}).items():
        compiled = [re.compile(p) for p in patterns]
        subset = [c for c in channels if any(p.search(c.get("final_group", "")) for p in compiled)]
        write_atomic(path, render_m3u(subset, config))
        print(f"[export] {path}: {len(subset):,} channels")


def count_existing_channels(path):
    if not os.path.exists(path):
        return 0
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            return sum(1 for line in fh if line.startswith("#EXTINF"))
    except Exception:
        return 0


def write_reports(channels, dead_channels, source_status, elapsed, config, health):
    os.makedirs(REPORTS_DIR, exist_ok=True)

    groups = Counter(c.get("final_group", "General") for c in channels)
    categories = Counter(c.get("category", "General") for c in channels)
    countries = Counter(c.get("country_code") or "unknown" for c in channels)

    stats = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "elapsed_seconds": round(elapsed, 1),
        "total_channels": len(channels),
        "kept_on_grace": sum(1 for c in channels if c.get("grace")),
        "kept_geo_blocked": sum(1 for c in channels if c.get("geo_blocked")),
        "dead_streams": len(dead_channels),
        "groups": dict(groups.most_common()),
        "categories": dict(categories.most_common()),
        "countries": dict(countries.most_common()),
        "sources": source_status,
    }
    write_atomic(STATS_PATH, json.dumps(stats, indent=2, ensure_ascii=False))

    dead_lines = [f"# {stats['generated_at']} - {len(dead_channels)} dead streams", ""]
    for channel, reason in sorted(dead_channels, key=lambda item: item[0]["name"].lower()):
        dead_lines.append(f"{channel['name']}\t{reason}\t{channel['url']}")
    write_atomic(DEAD_PATH, "\n".join(dead_lines) + "\n")

    region_locked = [c for c in channels if c.get("geo_blocked")]
    lines = [
        f"# {stats['generated_at']} - {len(region_locked)} streams kept but NOT verifiable from CI.",
        "# These are South Asian channels whose servers refuse connections from outside the region.",
        "# They are expected to play in Pakistan/India. Set keep_geo_blocked=false in config.json to drop them.",
        "",
    ]
    for channel in sorted(region_locked, key=lambda c: (c["final_group"], c["name"].lower())):
        lines.append(f"{channel['final_group']}\t{channel['name']}\t{channel['geo_blocked']}\t{channel['url']}")
    write_atomic(REGION_LOCKED_PATH, "\n".join(lines) + "\n")

    write_atomic(SOURCES_PATH, json.dumps(source_status, indent=2, ensure_ascii=False))
    if health is not None:
        write_atomic(HEALTH_PATH, json.dumps(health, indent=1, ensure_ascii=False))

    print(f"[report] {STATS_PATH}, {DEAD_PATH}, {REGION_LOCKED_PATH}, {SOURCES_PATH}, {HEALTH_PATH}")
    print("[report] top groups: " + ", ".join(f"{g}={n}" for g, n in groups.most_common(12)))


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

async def collect_channels(config, source_status):
    """Fetch every configured source and return the raw channel list."""
    timeout = config["source_timeout"]
    retries = config["source_retries"]
    sources = config["sources"]
    collected = []

    connector = aiohttp.TCPConnector(limit=20, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        jobs = {}
        if sources.get("iptv_org_api", True):
            for part in ("channels", "streams", "logos"):
                jobs[f"api:{part}"] = fetch_json(
                    session, f"{IPTV_ORG_API}/{part}.json", f"iptv-org/{part}", timeout, retries
                )
        for name, url in sources.get("m3u", {}).items():
            jobs[f"m3u:{name}"] = fetch_text(session, url, name, timeout, retries)

        results = dict(zip(jobs.keys(), await asyncio.gather(*jobs.values())))

    api_channels, channels_error = results.get("api:channels", (None, "not requested"))
    api_streams, streams_error = results.get("api:streams", (None, "not requested"))
    api_logos, _ = results.get("api:logos", (None, ""))

    if api_channels and api_streams:
        parsed = build_from_api(api_channels, api_streams, api_logos)
        collected.extend(parsed)
        source_status["iptv-org-api"] = {"ok": True, "channels": len(parsed)}
        print(f"[parse] iptv-org API: {len(parsed):,} streams")
    elif sources.get("iptv_org_api", True):
        source_status["iptv-org-api"] = {"ok": False, "error": channels_error or streams_error}

    for name in sources.get("m3u", {}):
        text, error = results[f"m3u:{name}"]
        if not text:
            source_status[name] = {"ok": False, "error": error}
            continue
        parsed = parse_m3u(text, name)
        collected.extend(parsed)
        source_status[name] = {"ok": True, "channels": len(parsed)}
        print(f"[parse] {name}: {len(parsed):,} channels")

    return collected


async def main():
    parser = argparse.ArgumentParser(description="Build the IPTV playlist.")
    parser.add_argument("--no-verify", action="store_true", help="skip stream probing (fast dry run)")
    parser.add_argument("--limit", type=int, default=0, help="only process the first N candidates")
    args = parser.parse_args()

    started = time.time()
    config = load_config()
    rules = compile_group_rules(config)
    source_status = {}

    print("=" * 68)
    print("IPTV playlist builder")
    print(f"home countries : {', '.join(config['countries'])}")
    print(f"group rules    : {len(rules)}")
    print("=" * 68)

    raw = await collect_channels(config, source_status)
    ok_sources = sum(1 for s in source_status.values() if s.get("ok"))
    print(f"[parse] {len(raw):,} raw entries from {ok_sources}/{len(source_status)} sources")

    if not raw:
        print("[fatal] every source failed; leaving the existing playlist untouched.")
        return 1

    excludes = [re.compile(p) for p in config.get("exclude_patterns", [])]
    candidates = []
    dropped_excluded = dropped_drm = dropped_unclassified = 0
    for channel in raw:
        haystack = f"{channel.get('name', '')} {channel.get('group_title', '')}"
        if any(p.search(haystack) for p in excludes):
            dropped_excluded += 1
            continue
        if config.get("drop_drm_channels", True) and channel.get("drm"):
            dropped_drm += 1
            continue
        if not classify(channel, config, rules):
            dropped_unclassified += 1
            continue
        candidates.append(channel)

    print(f"[filter] kept {len(candidates):,} (excluded {dropped_excluded:,}, "
          f"DRM {dropped_drm:,}, off-target {dropped_unclassified:,})")

    candidates = dedupe(candidates, config)
    print(f"[dedupe] {len(candidates):,} candidates after URL dedupe and "
          f"{config['max_streams_per_channel']}-stream-per-channel cap")

    if args.limit:
        candidates = candidates[:args.limit]
        print(f"[limit] truncated to {len(candidates):,}")

    health = load_health()

    if args.no_verify:
        alive, dead_map = candidates, {}
    else:
        alive, dead_map = await verify_channels(candidates, config)
        print(f"[verify] {len(alive):,} alive, {len(dead_map):,} dead")

    dead_channels = [(c, dead_map[c["url"]]) for c in candidates if c["url"] in dead_map]

    geo_blocked, dead_channels = split_geo_blocked(dead_channels, health, config)
    if geo_blocked:
        print(f"[geo] keeping {len(geo_blocked):,} South Asian streams that are unreachable "
              f"from this network but are expected to work in-region")

    revived, new_health = apply_grace(alive, geo_blocked, dead_channels, health,
                                      config.get("grace_failures", 2))
    if args.no_verify:
        # Nothing was actually probed, so the history must not be rewritten.
        new_health = None
    if revived:
        print(f"[grace] keeping {len(revived):,} streams that failed this run but worked recently")
    final = alive + geo_blocked + revived

    # Safety gate: never ship a collapsed playlist.
    previous = count_existing_channels(PLAYLIST_PATH)
    safety = config.get("safety", {})
    floor = max(
        safety.get("min_channels", 0),
        int(previous * (1 - safety.get("max_drop_ratio", 1.0))),
    )
    if previous and len(final) < floor:
        print(f"[fatal] only {len(final):,} channels survived vs {previous:,} previously "
              f"(floor {floor:,}). Refusing to overwrite - probably a network or upstream problem.")
        os.makedirs(REPORTS_DIR, exist_ok=True)
        write_atomic(STATS_PATH, json.dumps({
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "aborted": True,
            "would_have_written": len(final),
            "previous": previous,
            "floor": floor,
            "sources": source_status,
        }, indent=2))
        return 2

    if config["sort_channels"]:
        final.sort(key=group_sorter(config))

    export(final, config)
    write_reports(final, dead_channels, source_status, time.time() - started, config, new_health)
    print(f"Done in {time.time() - started:.1f}s")
    return 0


if __name__ == "__main__":
    # No selector-loop override on Windows: select() caps out at 512 handles and
    # dies partway through a large verification pass. The default proactor loop
    # has no such limit and aiohttp is happy on it.
    sys.exit(asyncio.run(main()))
