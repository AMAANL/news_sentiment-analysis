import requests
import zipfile
import io
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time
import logging
import json

log = logging.getLogger(__name__)

GDELT_BASE = "http://data.gdeltproject.org/gdeltv2/"

GKG_COLS = {
    0:  "date",
    3:  "source",
    4:  "url",
    7:  "themes",
    15: "tone_csv",
}

KEYWORDS = [
    "federal reserve", "interest rate", "inflation", "fed",
    "election", "vote", "congress", "senate",
    "nvidia", "tech stock", "nasdaq", "artificial intelligence",
    "recession", "gdp", "unemployment", "economy",
]


def fetch_gdelt_gkg_file(url: str) -> pd.DataFrame:
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        fname = z.namelist()[0]
        # Memory Optimization: Use chunksize to read large CSVs
        chunks = pd.read_csv(
            z.open(fname), sep="\t", header=None,
            usecols=list(GKG_COLS.keys()),
            names=list(GKG_COLS.values()),
            on_bad_lines="skip",
            low_memory=False,
            chunksize=50000,
            encoding_errors="replace"
        )
        
        filtered_results = []
        for chunk in chunks:
            # Filter chunk immediately to save RAM
            chunk = chunk[chunk['url'].notna()]
            mask = chunk.apply(lambda r: any(kw in str(r['themes']).lower() or kw in str(r['url']).lower() for kw in KEYWORDS), axis=1)
            filtered_results.append(chunk[mask])
            
        if not filtered_results:
            return pd.DataFrame()
        return pd.concat(filtered_results)
    except Exception as e:
        log.warning(f"Failed to fetch {url}: {e}")
        return pd.DataFrame()


def collect_historical_windows(target_total: int = 10000):
    windows = [
        ("Tech Sell-off",   "20240725000000", "20240815000000"),
        ("Fed Rate Cut",    "20240910000000", "20241001000000"),
        ("Election Window", "20241025000000", "20241115000000")
    ]
    
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    out_file = "data/raw/articles_urls_hist.json"
    
    # Checkpoint loading
    if Path(out_file).exists():
        with open(out_file, 'r') as f:
            all_articles = json.load(f)
            seen_urls = {a['url'] for a in all_articles}
            log.info(f"Loaded {len(all_articles)} existing URLs from checkpoint.")
    else:
        all_articles = []
        seen_urls = set()
    
    target_per_window = target_total // 3
    
    for name, start_str, end_str in windows:
        current_dt = datetime.strptime(start_str, "%Y%m%d%H%M%S")
        end_dt = datetime.strptime(end_str, "%Y%m%d%H%M%S")
        
        window_articles = 0
        log.info(f"--- Starting REAL collection for {name} ---")
        
        while current_dt <= end_dt and window_articles < target_per_window:
            ts = current_dt.strftime("%Y%m%d%H%M%S")
            url = f"{GDELT_BASE}{ts}.gkg.csv.zip"
            
            df = fetch_gdelt_gkg_file(url)
            log.info(f"Fetching {ts} - Window Total: {window_articles}")
            
            if not df.empty:
                for _, row in df.iterrows():
                    u = str(row['url']).strip()
                    if u in seen_urls or not u.startswith("http"):
                        continue
                    seen_urls.add(u)
                    
                    try:
                        date_str = str(row["date"])[:8]
                        published = datetime.strptime(date_str, "%Y%m%d").isoformat() + "Z"
                    except:
                        published = None

                    all_articles.append({
                        "url": u,
                        "source": str(row.get("source", "")).strip(),
                        "headline": "",
                        "published_at": published,
                        "scrape_at": datetime.utcnow().isoformat() + "Z",
                        "body": "",
                        "themes": str(row.get("themes", "")),
                    })
                    window_articles += 1
            
            # Save progress every file to avoid loss
            with open(out_file, 'w') as f:
                json.dump(all_articles, f, indent=2)
                
            current_dt += timedelta(hours=6) # 6 hour samples to cover the windows broadly
            time.sleep(1)

    log.info(f"Completed REAL 2024 URL Collection: {len(all_articles)} URLs gathered.")
    return len(all_articles)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    collect_historical_windows()
