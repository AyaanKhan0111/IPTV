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
        "max_threads": 80,
        "verify_with_ffprobe": False,
        "remove_duplicates": True,
        "sort_channels": True,
        "favorites": ["PTV Sports", "PTV Home", "Geo News", "Aaj News", "DD National", "DD News", "ABP News", "India TV"],
        "categories": ["News", "Sports", "Entertainment", "Religious", "Music", "Kids", "Documentary"],
        "sports_countries": ["us", "uk", "ca", "au", "nz", "za", "ie"],
        "sports_keywords": ["tsn", "sky.*sport", "star.*sport", "willow", "ptv.*sport", "ten.*sport", "sony.*ten", "super.*sport", "eurosport", "espn", "fox.*sport", "bein", "dazn"],
        "include_us_uk_movies_entertainment": True
    }
    
    if not os.path.exists(config_path):
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

def parse_m3u(text, source_name=""):
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
            current_info["source"] = source_name
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
    if "documentary" in og or "science" in og or "history" in og or "nature" in og:
        return "Documentary"
    if "entertainment" in og or "comedy" in og or "drama" in og or "series" in og or "show" in og:
        return "Entertainment"
        
    return "General"

def check_with_ffprobe(url, timeout_sec, headers=None):
    global FFPROBE_AVAILABLE
    if not FFPROBE_AVAILABLE:
        return False
        
    try:
        timeout_us = int(timeout_sec * 1000000)
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
    
    for opt in channel.get("opts", []):
        if "http-user-agent=" in opt:
            headers["User-Agent"] = opt.split("http-user-agent=")[1].strip()
        elif "http-referrer=" in opt:
            headers["Referer"] = opt.split("http-referrer=")[1].strip()
            
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout), ssl=False) as response:
            if response.status != 200:
                return False
                
            try:
                content_chunk = await response.content.read(512)
                if not content_chunk:
                    return False
                content_str = content_chunk.decode('utf-8', errors='ignore').strip()
                
                # Filter out HTML portals/parking pages
                if content_str.startswith(("<!DOCTYPE", "<!doctype", "<html", "<HTML", "<?xml")):
                    return False
                    
                # Verify M3U8 files contain the standard tag
                content_type = response.headers.get("Content-Type", "").lower()
                is_m3u8 = "m3u8" in url.lower() or "vnd.apple.mpegurl" in content_type or "x-mpegurl" in content_type
                if is_m3u8 and "#EXTM3U" not in content_str:
                    return False
            except Exception:
                return False
                
            if verify_with_ffprobe and FFPROBE_AVAILABLE:
                ffprobe_success = await asyncio.to_thread(check_with_ffprobe, url, timeout, headers)
                if FFPROBE_AVAILABLE:
                    return ffprobe_success
                    
            return True
    except Exception:
        return False

async def verify_channels(channels, timeout, max_threads, verify_with_ffprobe):
    print(f"Verifying {len(channels)} channels concurrently (max concurrent: {max_threads})...")
    connector = aiohttp.TCPConnector(limit=max_threads, ssl=False)
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
        
        for future in asyncio.as_completed(tasks):
            channel, success = await future
            completed += 1
            if success:
                verified_channels.append(channel)
            if completed % 25 == 0 or completed == total:
                print(f"Progress: {completed}/{total} verified ({len(verified_channels)} active)...")
                
        return verified_channels

async def fetch_playlist(session, url, name):
    print(f"Fetching {name} from {url}...")
    try:
        async with session.get(url, timeout=20, ssl=False) as r:
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
    seen_urls = set()
    
    # Sports pattern matcher
    sports_patterns = [re.compile(pat, re.IGNORECASE) for pat in sports_keywords]
    
    # Premium keywords (very complete, covering all requested channels and variants)
    premium_keywords = config.get("premium_keywords", [
        "hbo", "amc", "adult.*swim", "showtime", "starz", "cinemax",
        "\\bfx\\b", "\\bfxx\\b", "\\bfxm\\b", "\\btbs\\b", "\\btnt\\b",
        "usa.*network", "paramount", "syfy", "comedy.*central", "\\bcw\\b",
        "sky.*atlantic", "sky.*cinema", "sky.*max", "sky.*showcase",
        "bbc.*one", "bbc.*two", "bbc.*three", "bbc.*four", "\\bitv\\b",
        "channel.*4", "\\be4\\b", "more4",
        "discovery", "national.*geographic", "nat.*geo", "\\bhistory\\b", "\\baxn\\b",
        "espn", "sony", "colors", "star.*plus", "pogo", "boomerang", "tsn", "willow", 
        "sky.*sport", "super.*sport", "bein", "fox.*sport", "\\bcbs\\b", "\\babc\\b", "\\bnbc\\b",
        "disney", "cartoon.*network", "nickelodeon", "nick", "hgtv", "food.*network", "tlc", "dazn"
    ])
    premium_patterns = [re.compile(pat, re.IGNORECASE) for pat in premium_keywords]
    
    # Helper to guess country
    def guess_country(ch_obj):
        # 1. Check tvg_id ending
        tvg_id_val = ch_obj.get("tvg_id", "")
        match_val = re.search(r'\.([a-z]{2})\b|\.([a-z]{2})@', tvg_id_val, re.IGNORECASE)
        if match_val:
            cc = (match_val.group(1) or match_val.group(2)).lower()
            if cc in ["pk", "in", "us", "uk", "ca", "au", "nz", "za", "ie", "ae", "sa"]:
                return cc
                
        # 2. Check source name
        source_val = ch_obj.get("source", "")
        if source_val in ["pk", "in", "us", "uk", "ca", "au", "nz", "za", "ie", "ae", "sa"] or source_val in ["au_mjh", "nz_mjh"]:
            return "au" if "au" in source_val else ("nz" if "nz" in source_val else source_val)
        if source_val == "hindi_punjabi":
            return "in"
            
        # 3. Check group title
        grp_val = ch_obj.get("group_title", "").lower()
        grp_val = re.sub(r'\[?(non[- ])?geo[- ]blocked\]?', '', grp_val)
        if "pakistan" in grp_val or "urdu" in grp_val:
            return "pk"
        if any(term in grp_val for term in ["india", "hindi", "punjabi", "tamil", "telugu", "malayalam", "kannada", "bengali", "marathi", "gujarati"]):
            return "in"
        if "united states" in grp_val or "usa" in grp_val:
            return "us"
        if any(term in grp_val for term in ["united kingdom", "uk", "great britain"]):
            return "uk"
        if "canada" in grp_val or "ca" in grp_val:
            return "ca"
        if "australia" in grp_val or "au" in grp_val:
            return "au"
        if "new zealand" in grp_val or "nz" in grp_val:
            return "nz"
        if "south africa" in grp_val or "za" in grp_val:
            return "za"
        if "ireland" in grp_val or "ie" in grp_val:
            return "ie"
            
        # 4. Check name
        name_val = ch_obj.get("name", "").lower()
        name_val = re.sub(r'\[?(non[- ])?geo[- ]blocked\]?', '', name_val)
        pk_keywords = [r"\bary\b", r"\bgeo\b", r"\bhum\b", r"\bptv\b", r"\bten\s*sport", r"\bexpress\s*news", r"\bexpress\s*entertainment", r"\bdunya\s*news", r"\bsamaa\b", r"\b92\s*news", r"\bgnn\b", r"\bpakistan"]
        if any(re.search(pat, name_val) for pat in pk_keywords):
            return "pk"
            
        in_keywords = [r"\bdd\b", r"\bsony\b", r"\bcolors\b", r"\bstar\b", r"\bzee\b", r"\bpogo\b", r"\bsun\s*tv", r"\betv\b", r"\bgemini\b", r"\bsurya\b", r"\budaya\b", r"\basianet\b", r"\bmazhavil\b", r"\bkairali\b", r"\bamrita\b", r"\bflowers\s*tv"]
        if any(re.search(pat, name_val) for pat in in_keywords):
            return "in"
            
        return ""
    
    for ch in channels:
        tvg_id = ch["tvg_id"]
        name = ch["name"]
        url = ch["url"]
        
        if config["remove_duplicates"]:
            if url in seen_urls:
                continue
            seen_urls.add(url)
            
        # Always keep manual/verified channels
        if ch.get("source") == "manual":
            grp = ch.get("group_title", "Entertainment")
            ch["final_group"] = grp
            # Check if it is Indian and should be combined under Entertainment
            if "indian" in grp.lower() and any(term in grp.lower() for term in ["movies", "music", "entertainment", "general"]):
                ch["final_group"] = "Indian Entertainment"
                ch["category"] = "Entertainment"
            else:
                ch["category"] = grp.split()[-1] if len(grp.split()) > 1 else grp
            selected_channels.append(ch)
            continue
            
        country_code = guess_country(ch)
        
        is_target_country = country_code in countries
        original_group = ch["group_title"]
        clean_category = get_clean_category(original_group, categories_config)
        
        # Check if it matches sports keywords
        is_sports_channel = clean_category == "Sports" or original_group.lower() == "sports"
        matches_sports_keywords = any(pat.search(tvg_id + " " + name) for pat in sports_patterns)
        if matches_sports_keywords:
            is_sports_channel = True
            
        # 1. Keep ALL working Pakistani and Indian channels regardless of category
        if is_target_country:
            if is_sports_channel:
                clean_category = "Sports"
            ch["category"] = clean_category
            
            country_name = "Pakistani" if country_code == "pk" else "Indian"
            
            # Map category
            is_kids = clean_category == "Kids" or any(k in name.lower() for k in ["kid", "pogo", "boomerang", "disney", "nick", "cartoon"])
            is_doc = clean_category == "Documentary" or any(d in name.lower() for d in ["documentary", "history", "discovery", "geographic", "geo"])
            is_movie = clean_category == "Movies" or any(m in name.lower() or m in original_group.lower() 
                                                         for m in ["movie", "film", "cinema", "hbo", "starz", "showtime", "cinemax"])
            
            if is_kids:
                clean_category = "Kids"
            elif is_doc:
                clean_category = "Documentary"
            elif is_movie:
                clean_category = "Movies"
                
            ch["category"] = clean_category
            
            # Combine Indian Entertainment, Movies, Music, Drama into Indian Entertainment
            if country_code == "in" and clean_category in ["Movies", "Music", "Entertainment", "General"]:
                ch["category"] = "Entertainment"
                ch["final_group"] = "Indian Entertainment"
            else:
                ch["final_group"] = f"{country_name} {clean_category}"
                
            selected_channels.append(ch)
            
        # 2. Keep international sports channels
        elif is_sports_channel:
            is_sports_country = country_code in sports_countries
            is_asian = country_code not in ["us", "uk", "ca", "de", "fr", "it", "es", "au", "nz", "za", "br", "mx", "ar"]
            
            if is_sports_country and matches_sports_keywords:
                ch["category"] = "Sports"
                ch["final_group"] = "Sports (International)"
                selected_channels.append(ch)
            elif is_asian:
                ch["category"] = "Sports"
                ch["final_group"] = "Sports (Asia)"
                selected_channels.append(ch)
                
        # 3. Keep other major premium entertainment / documentary / movies / kids channels
        elif (country_code in ["us", "uk", "ca", "au", "nz", "za", "ie"] or "mbc" in name.lower() or "cw" in name.lower()) and config.get("include_us_uk_movies_entertainment", True):
            matches_premium = any(pat.search(tvg_id + " " + name) for pat in premium_patterns) or "mbc" in name.lower() or "cw" in name.lower()
            
            if clean_category in ["Movies", "Entertainment", "Kids", "Documentary"] or matches_premium:
                if "mbc" in name.lower():
                    country_name = "MBC"
                elif "cw" in name.lower() and country_code not in ["us", "uk", "ca", "au", "nz", "za", "ie"]:
                    country_name = "US"
                else:
                    country_name = country_code.upper()
                
                # Map categories nicely
                is_kids = clean_category == "Kids" or any(k in name.lower() for k in ["kid", "pogo", "boomerang", "disney", "nick", "cartoon"])
                is_doc = clean_category == "Documentary" or any(d in name.lower() for d in ["documentary", "history", "discovery", "geographic", "geo"])
                is_movie = clean_category == "Movies" or any(m in name.lower() or m in original_group.lower() 
                                                            for m in ["movie", "film", "cinema", "hbo", "starz", "showtime", "cinemax"])
                
                if is_kids:
                    cat = "Kids"
                elif is_doc:
                    cat = "Documentary"
                elif is_movie:
                    cat = "Movies"
                else:
                    cat = "Entertainment"
                    
                ch["category"] = cat
                ch["final_group"] = f"{country_name} {cat}"
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
        "Pakistani Documentary",
        "Pakistani Kids",
        "Pakistani Religious",
        "Pakistani Music",
        "Pakistani General",
        
        "Indian News",
        "Indian Entertainment",
        "Indian Movies",
        "Indian Documentary",
        "Indian Kids",
        "Indian Religious",
        "Indian Music",
        "Indian General",
        
        "MBC Movies",
        "MBC Entertainment",
        "MBC Documentary",
        "MBC Kids",
        
        "US Movies",
        "US Entertainment",
        "US Documentary",
        "US Kids",
        
        "UK Movies",
        "UK Entertainment",
        "UK Documentary",
        "UK Kids",
        
        "CA Movies",
        "CA Entertainment",
        "CA Documentary",
        "CA Kids",
        
        "AU Movies",
        "AU Entertainment",
        "AU Documentary",
        "AU Kids",
        
        "NZ Movies",
        "NZ Entertainment",
        "NZ Documentary",
        "NZ Kids",
        
        "ZA Movies",
        "ZA Entertainment",
        "ZA Documentary",
        "ZA Kids",
        
        "IE Movies",
        "IE Entertainment",
        "IE Documentary",
        "IE Kids"
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
        
        name_lower = ch["name"].lower()
        is_fav = any(fav in name_lower for fav in favorites)
        fav_val = 0 if is_fav else 1
        
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
                
                f.write(f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{tvg_name}" tvg-logo="{logo}" group-title="{group}",{display_name}\n')
                for opt in ch.get("opts", []):
                    f.write(f"{opt}\n")
                f.write(f"{ch['url']}\n")
        print(f"Successfully exported {len(channels)} channels to {output_path}")
    except Exception as e:
        print(f"Error exporting playlist to {output_path}: {e}")

async def main():
    start_time = time.time()
    config = load_config()
    
    print("Initializing Experimental IPTV Playlist Builder 2...")
    print(f"Targeting Countries: {config['countries']}")
    print(f"Timeout: {config['timeout']}s, Max Threads: {config['max_threads']}")
    
    urls = {
        "pk": "https://iptv-org.github.io/iptv/countries/pk.m3u",
        "in": "https://iptv-org.github.io/iptv/countries/in.m3u",
        "us": "https://iptv-org.github.io/iptv/countries/us.m3u",
        "uk": "https://iptv-org.github.io/iptv/countries/uk.m3u",
        "ca": "https://iptv-org.github.io/iptv/countries/ca.m3u",
        "au": "https://iptv-org.github.io/iptv/countries/au.m3u",
        "nz": "https://iptv-org.github.io/iptv/countries/nz.m3u",
        "za": "https://iptv-org.github.io/iptv/countries/za.m3u",
        "ie": "https://iptv-org.github.io/iptv/countries/ie.m3u",
        "ae": "https://iptv-org.github.io/iptv/countries/ae.m3u",
        "sa": "https://iptv-org.github.io/iptv/countries/sa.m3u",
        "au_mjh": "https://i.mjh.nz/au/raw.m3u8",
        "nz_mjh": "https://i.mjh.nz/nz/raw.m3u8",
        "sports": "https://iptv-org.github.io/iptv/categories/sports.m3u",
        "documentary": "https://iptv-org.github.io/iptv/categories/documentary.m3u",
        "kids": "https://iptv-org.github.io/iptv/categories/kids.m3u",
        "entertainment": "https://iptv-org.github.io/iptv/categories/entertainment.m3u",
        "movies": "https://iptv-org.github.io/iptv/categories/movies.m3u",
        "hindi_punjabi": "https://raw.githubusercontent.com/deep2772/Hindi_Punjabi-iptv-playlist/refs/heads/main/Hindi_Punjabi_Merged.m3u",
        "curated": "https://raw.githubusercontent.com/Rayyan98/my-curated-iptv/refs/heads/main/real_data/main_working.m3u"
    }
    
    raw_playlists = {}
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_playlist(session, url, name) for name, url in urls.items()]
        results = await asyncio.gather(*tasks)
        for name, text in zip(urls.keys(), results):
            raw_playlists[name] = text
            
    all_parsed_channels = []
    for name, text in raw_playlists.items():
        if text:
            parsed = parse_m3u(text, name)
            print(f"Parsed {len(parsed)} channels from {name} playlist.")
            all_parsed_channels.extend(parsed)
            
    import os
    import json
    
    manual_channels = [
        {
            "tvg_id": "DAZN5.uk",
            "tvg_name": "DAZN 5",
            "logo": "https://upload.wikimedia.org/wikipedia/commons/e/e9/DAZN_Logo.svg",
            "group_title": "Sports",
            "name": "DAZN 5 (1080p)",
            "source": "manual",
            "url": "http://znty.dyndns.org:5010/hls/eleven5.m3u8",
            "opts": []
        },
        {
            "tvg_id": "WCIUTV265.us",
            "tvg_name": "The CW (WCIU)",
            "logo": "https://upload.wikimedia.org/wikipedia/commons/3/3d/The_CW_logo.svg",
            "group_title": "Entertainment",
            "name": "The CW (WCIU)",
            "source": "manual",
            "url": "https://2-fss-1.streamhoster.com/pl_122/206858-4412976-1/chunklist.m3u8",
            "opts": []
        }
    ]
    
    if os.path.exists("working_new_channels.json"):
        try:
            with open("working_new_channels.json", "r", encoding="utf-8") as f:
                new_working = json.load(f)
            print(f"Loaded {len(new_working)} custom verified target channels from working_new_channels.json")
            for w in new_working:
                name = w["name"]
                group = w["group"]
                ch_id = w["id"]
                url = w["url"]
                
                is_sports = "sport" in group.lower() or "sport" in name.lower() or "dazn" in name.lower() or "gametoon" in name.lower() or "esport" in name.lower() or "esport" in group.lower() or "sportklub" in name.lower()
                
                if is_sports:
                    final_grp = "Sports (International)"
                else:
                    # Guess country from id or name
                    cc = "in" # Default to IN for these premium entertainment channels
                    if ch_id.endswith(".pk") or "pakistan" in name.lower() or any(p in name.lower() for p in ["ary", "hum", "geo", "express", "green tv"]):
                        cc = "pk"
                    elif ch_id.endswith(".us") or ch_id.endswith(".uk") or ch_id.endswith(".ca") or ch_id.endswith(".au") or ch_id.endswith(".nz") or ch_id.endswith(".za") or ch_id.endswith(".ie"):
                        cc = ch_id.split(".")[-1].split("@")[0].lower()
                    else:
                        # Default to US/UK for premium international channels like BBC Earth, History, Discovery, Fox Life
                        cc = "uk" if "bbc" in name.lower() else "us"
                    
                    # Map category
                    is_kids = any(k in name.lower() for k in ["kid", "pogo", "boomerang", "disney", "nick", "cartoon"])
                    is_doc = "history" in group.lower() or "discovery" in group.lower() or "earth" in group.lower() or "planet" in group.lower()
                    is_movie = any(m in name.lower() for m in ["movie", "film", "cinema", "goldmines", "romance", "select", "thrills"])
                    
                    if is_kids:
                        cat = "Kids"
                    elif is_doc:
                        cat = "Documentary"
                    elif is_movie:
                        cat = "Movies"
                    else:
                        cat = "Entertainment"
                    
                    # Map country name
                    if cc == "pk":
                        cname = "Pakistani"
                    elif cc == "in":
                        cname = "Indian"
                    else:
                        cname = cc.upper()
                    
                    final_grp = f"{cname} {cat}"
                
                manual_channels.append({
                    "tvg_id": ch_id,
                    "tvg_name": name,
                    "logo": "",
                    "group_title": final_grp,
                    "name": f"{name} (1080p)" if "1080p" not in name else name,
                    "source": "manual",
                    "url": url,
                    "opts": []
                })
        except Exception as e:
            print(f"Error loading custom verified channels: {e}")
            
    all_parsed_channels.extend(manual_channels)
            
    print(f"Total parsed channels: {len(all_parsed_channels)}")
    
    filtered = filter_channels(all_parsed_channels, config)
    print(f"Filtered down to {len(filtered)} potential target channels.")
    
    verified = await verify_channels(
        filtered, 
        timeout=config["timeout"], 
        max_threads=config["max_threads"], 
        verify_with_ffprobe=config["verify_with_ffprobe"]
    )
    print(f"Verified {len(verified)} active channels.")
    
    if config["sort_channels"]:
        sorted_channels = sort_channels(verified, config)
    else:
        sorted_channels = verified
        
    export_m3u(sorted_channels, "playlist.m3u")
    export_m3u(sorted_channels, "playlists/SouthAsia.m3u")
    
    elapsed = time.time() - start_time
    print(f"Done! Completed in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
