import asyncio
import aiohttp
import json
import re
import os
import sys
import subprocess
import time
from urllib.parse import urlparse

# Global flag to check if ffprobe is working
FFPROBE_AVAILABLE = True

def load_config():
    config_path = "config.json"
    default_config = {
        "countries": ["pk", "in"],
        "timeout": 8,
        "max_threads": 50,
        "verify_with_ffprobe": True,
        "remove_duplicates": True,
        "sort_channels": True,
        "favorites": ["PTV Sports", "PTV Home", "Geo News", "Aaj News", "DD National", "DD News", "ABP News", "India TV"],
        "categories": ["News", "Sports", "Entertainment", "Religious", "Music", "Kids"],
        "sports_countries": ["us", "uk", "ca"],
        "sports_keywords": ["tsn", "sky.*sport", "star.*sport", "willow", "ptv.*sport", "ten.*sport", "sony.*ten", "super.*sport", "eurosport", "espn"]
    }
    
    if not os.path.exists(config_path):
        print(f"Config file {config_path} not found. Using defaults.")
        return default_config
        
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            user_config = json.load(f)
            # Merge user config with defaults
            for k, v in default_config.items():
                if k not in user_config:
                    user_config[k] = v
            return user_config
    except Exception as e:
        print(f"Error loading config.json: {e}. Using defaults.")
        return default_config

def parse_extinf(line):
    # Extract display name (after the last comma)
    comma_idx = line.rfind(",")
    if comma_idx != -1:
        display_name = line[comma_idx+1:].strip()
        attributes_part = line[:comma_idx]
    else:
        display_name = ""
        attributes_part = line
        
    # Extract key-value pairs using regex
    kv_pattern = re.compile(r'([\w-]+)="([^"]*)"')
    attributes = dict(kv_pattern.findall(attributes_part))
    
    return {
        "tvg_id": attributes.get("tvg-id", ""),
        "tvg_name": attributes.get("tvg-name", display_name),
        "logo": attributes.get("tvg-logo", ""),
        "group_title": attributes.get("group-title", "General"),
        "name": display_name
    }

def parse_m3u(text):
    channels = []
    lines = text.splitlines()
    current_info = None
    current_opts = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("#EXTM3U"):
            continue
        elif line.startswith("#EXTINF"):
            current_info = parse_extinf(line)
            current_opts = []
        elif line.startswith("#EXTVLCOPT"):
            current_opts.append(line)
        elif not line.startswith("#") and (line.startswith("http://") or line.startswith("https://")):
            if current_info:
                current_info["url"] = line
                current_info["opts"] = current_opts
                channels.append(current_info)
            current_info = None
            current_opts = []
            
    return channels

def get_clean_category(original_group, categories_config):
    og = original_group.lower()
    for cat in categories_config:
        if cat.lower() in og:
            return cat
            
    if "movie" in og or "film" in og:
        return "Movies"
    if "music" in og or "song" in og:
        return "Music"
    if "kid" in og or "cartoon" in og or "animation" in og:
        return "Kids"
    if "religion" in og or "spiritual" in og or "islam" in og or "christian" in og or "hindu" in og or "sikh" in og:
        return "Religious"
    if "news" in og or "business" in og or "weather" in og:
        return "News"
    if "sport" in og:
        return "Sports"
    if "entertainment" in og or "comedy" in og or "drama" in og or "series" in og or "show" in og:
        return "Entertainment"
        
    return "General"

def check_with_ffprobe(url, timeout_sec, headers=None):
    global FFPROBE_AVAILABLE
    if not FFPROBE_AVAILABLE:
        return False
        
    try:
        # Timeout in microseconds for ffprobe
        timeout_us = int(timeout_sec * 1000000)
        
        # Build headers string for ffmpeg
        headers_str = ""
        if headers:
            for k, v in headers.items():
                headers_str += f"{k}: {v}\r\n"
                
        cmd = [
            "ffprobe",
            "-v", "error",
        ]
        
        if headers_str:
            cmd.extend(["-headers", headers_str])
            
        cmd.extend([
            "-show_entries", "format=format_name",
            "-of", "default=noprint_wrappers=1",
            "-timeout", str(timeout_us),
            url
        ])
        
        # Run ffprobe with process timeout
        result = subprocess.run(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True, 
            timeout=timeout_sec
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        return False
    except FileNotFoundError:
        print("WARNING: ffprobe command not found. Disabling ffprobe check.")
        FFPROBE_AVAILABLE = False
        return False
    except Exception:
        return False

async def check_stream(channel, session, timeout, verify_with_ffprobe):
    url = channel["url"]
    headers = {
        "User-Agent": "VLC/3.0.18 LibVLC/3.0.18"
    }
    
    # Process custom headers from EXTVLCOPT
    for opt in channel.get("opts", []):
        if "http-user-agent=" in opt:
            headers["User-Agent"] = opt.split("http-user-agent=")[1].strip()
        elif "http-referrer=" in opt:
            headers["Referer"] = opt.split("http-referrer=")[1].strip()
            
    # Step 1: HTTP Connection check (GET stream)
    try:
        # Use ClientTimeout for total connection + header response time
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status != 200:
                return False
                
            # If HTTP check is successful, try ffprobe if requested and available
            if verify_with_ffprobe and FFPROBE_AVAILABLE:
                # Run ffprobe in thread executor to avoid blocking the event loop
                ffprobe_success = await asyncio.to_thread(check_with_ffprobe, url, timeout, headers)
                # If ffprobe check runs successfully, return its status
                # If ffprobe is disabled due to missing command, we fall back to HTTP status (True)
                if FFPROBE_AVAILABLE:
                    return ffprobe_success
                    
            return True
    except Exception:
        return False

async def verify_channels(channels, timeout, max_threads, verify_with_ffprobe):
    print(f"Verifying {len(channels)} channels concurrently (max concurrent: {max_threads})...")
    
    # We use a TCPConnector with limit to avoid overloading
    connector = aiohttp.TCPConnector(limit=max_threads, ssl=False)
    # VLC or Chrome User Agent as fallback
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(max_threads)
        
        async def sem_check(channel):
            async with semaphore:
                success = await check_stream(channel, session, timeout, verify_with_ffprobe)
                return channel, success
                
        tasks = [sem_check(ch) for ch in channels]
        
        verified_channels = []
        completed = 0
        total = len(tasks)
        
        # Gather results as they complete
        for future in asyncio.as_completed(tasks):
            channel, success = await future
            completed += 1
            if success:
                verified_channels.append(channel)
            if completed % 10 == 0 or completed == total:
                print(f"Progress: {completed}/{total} verified ({len(verified_channels)} active)...")
                
        return verified_channels

async def fetch_playlist(session, url, name):
    print(f"Fetching {name} from {url}...")
    try:
        async with session.get(url, timeout=15) as r:
            if r.status == 200:
                text = await r.text()
                print(f"Fetched {name} successfully: {len(text)} chars.")
                return text
            else:
                print(f"Failed to fetch {name}: status code {r.status}")
                return ""
    except Exception as e:
        print(f"Error fetching {name}: {e}")
        return ""

def filter_channels(channels, config):
    countries = config["countries"]
    sports_countries = config["sports_countries"]
    sports_keywords = config["sports_keywords"]
    categories_config = config["categories"]
    
    selected_channels = []
    
    # Compile sports keyword regex patterns
    sports_patterns = [re.compile(pat, re.IGNORECASE) for pat in sports_keywords]
    
    # Track duplicates if configured
    seen_urls = set()
    
    for ch in channels:
        tvg_id = ch["tvg_id"]
        name = ch["name"]
        url = ch["url"]
        
        if config["remove_duplicates"]:
            if url in seen_urls:
                continue
            seen_urls.add(url)
            
        # Determine country ending
        country_code = ""
        # Match standard .cc or .cc@ in tvg_id
        match = re.search(r'\.([a-z]{2})\b|\.([a-z]{2})@', tvg_id, re.IGNORECASE)
        if match:
            # Get the non-empty group
            country_code = (match.group(1) or match.group(2)).lower()
            
        # 1. Check if Pakistani or Indian channel
        is_target_country = country_code in countries
        
        # 2. Check if international sports channel
        is_sports_channel = False
        original_group = ch["group_title"]
        clean_category = get_clean_category(original_group, categories_config)
        
        # Is it a sports channel from sports category?
        if clean_category == "Sports" or original_group.lower() == "sports":
            is_sports_channel = True
            
        # Does the name/id match sports keywords?
        matches_sports_keywords = any(pat.search(tvg_id + " " + name) for pat in sports_patterns)
        if matches_sports_keywords:
            is_sports_channel = True
            
        # Determine final inclusion and group-title mapping
        if is_target_country:
            # Map category
            if is_sports_channel:
                clean_category = "Sports"
            ch["category"] = clean_category
            
            # Map group title: e.g. "Pakistani News", "Indian Movies"
            country_name = "Pakistani" if country_code == "pk" else "Indian"
            ch["final_group"] = f"{country_name} {clean_category}"
            selected_channels.append(ch)
            
        elif is_sports_channel:
            # Include sports channel from US, UK, CA, or other Asian country
            # Check country_code against sports countries or if it is from asia.m3u (which implies Asian sports)
            # Wait, how to know if it's from asia.m3u? We will pass the source name, but we can also check if country is Asian.
            # Common Asian country codes: bd, np, lk, af, ae, sa, qa, etc.
            # Or if it matches sports keywords for US/UK/CA
            is_sports_country = country_code in sports_countries
            
            # Let's verify: if it matches sports keywords or is from US/UK/CA/Asia
            # To be safe: we include if it is from US/UK/CA and matches keywords, OR if it's from any Asian country and is sports.
            is_asian = country_code not in ["us", "uk", "ca", "de", "fr", "it", "es", "au", "nz", "za", "br", "mx", "ar"]
            
            if is_sports_country and matches_sports_keywords:
                ch["category"] = "Sports"
                ch["final_group"] = "Sports (International)"
                selected_channels.append(ch)
            elif is_asian:
                ch["category"] = "Sports"
                ch["final_group"] = "Sports (Asia)"
                selected_channels.append(ch)
                
    return selected_channels

def group_sort_key(group_title):
    order = [
        "Pakistani Sports",
        "Indian Sports",
        "Sports (International)",
        "Sports (Asia)",
        "Pakistani News",
        "Pakistani Entertainment",
        "Pakistani Movies",
        "Pakistani Religious",
        "Pakistani Kids",
        "Pakistani Music",
        "Pakistani General",
        "Indian News",
        "Indian Entertainment",
        "Indian Movies",
        "Indian Religious",
        "Indian Kids",
        "Indian Music",
        "Indian General"
    ]
    try:
        return order.index(group_title)
    except ValueError:
        return len(order)

def sort_channels(channels, config):
    favorites = [f.lower() for f in config["favorites"]]
    
    def channel_key(ch):
        group = ch.get("final_group", "General")
        group_idx = group_sort_key(group)
        
        # Check if favorite (name contains favorite string)
        name_lower = ch["name"].lower()
        is_fav = any(fav in name_lower for fav in favorites)
        fav_val = 0 if is_fav else 1 # 0 comes before 1
        
        return (group_idx, fav_val, ch["name"].lower())
        
    return sorted(channels, key=channel_key)

def export_m3u(channels, output_path):
    dir_name = os.path.dirname(output_path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for ch in channels:
                tvg_id = ch["tvg_id"]
                tvg_name = ch["tvg_name"]
                logo = ch["logo"]
                group = ch.get("final_group", "General")
                display_name = ch["name"]
                
                # Format EXTINF line
                f.write(f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{tvg_name}" tvg-logo="{logo}" group-title="{group}",{display_name}\n')
                
                # Write options
                for opt in ch.get("opts", []):
                    f.write(f"{opt}\n")
                    
                # Write URL
                f.write(f"{ch['url']}\n")
        print(f"Successfully exported {len(channels)} channels to {output_path}")
    except Exception as e:
        print(f"Error exporting playlist to {output_path}: {e}")

async def main():
    start_time = time.time()
    config = load_config()
    
    print("Initializing IPTV Playlist Builder...")
    print(f"Countries: {config['countries']}")
    print(f"Sports Countries: {config['sports_countries']}")
    print(f"FFprobe verification enabled: {config['verify_with_ffprobe']}")
    
    urls = {
        "asia": "https://iptv-org.github.io/iptv/regions/asia.m3u",
        "sports": "https://iptv-org.github.io/iptv/categories/sports.m3u",
        "us": "https://iptv-org.github.io/iptv/countries/us.m3u",
        "uk": "https://iptv-org.github.io/iptv/countries/uk.m3u",
        "ca": "https://iptv-org.github.io/iptv/countries/ca.m3u"
    }
    
    raw_playlists = {}
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_playlist(session, url, name) for name, url in urls.items()]
        results = await asyncio.gather(*tasks)
        for name, text in zip(urls.keys(), results):
            raw_playlists[name] = text
            
    # Parse all channels
    all_parsed_channels = []
    for name, text in raw_playlists.items():
        if text:
            parsed = parse_m3u(text)
            print(f"Parsed {len(parsed)} channels from {name} playlist.")
            all_parsed_channels.extend(parsed)
            
    print(f"Total parsed channels: {len(all_parsed_channels)}")
    
    # Filter channels based on rules
    filtered = filter_channels(all_parsed_channels, config)
    print(f"Filtered down to {len(filtered)} potential target channels.")
    
    # Verify stream playability
    verified = await verify_channels(
        filtered, 
        timeout=config["timeout"], 
        max_threads=config["max_threads"], 
        verify_with_ffprobe=config["verify_with_ffprobe"]
    )
    print(f"Verified {len(verified)} active channels.")
    
    # Sort channels
    if config["sort_channels"]:
        sorted_channels = sort_channels(verified, config)
    else:
        sorted_channels = verified
        
    # Export playlist
    output_path = "playlists/SouthAsia.m3u"
    export_m3u(sorted_channels, output_path)
    
    # Also export to main root directory for ease of access if needed
    export_m3u(sorted_channels, "playlist.m3u")
    
    elapsed = time.time() - start_time
    print(f"Done! Completed in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    # Handle Windows async loop policy for subprocesses
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
