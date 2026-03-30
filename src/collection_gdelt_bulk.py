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
    "tariff", "trade war", "climate", "healthcare",
    "oil price", "crypto", "bitcoin", "housing market",
]


def fetch_gdelt_gkg_file(url: str) -> pd.DataFrame:
    """Download and parse one GDELT GKG CSV zip."""
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        fname = z.namelist()[0]
        with z.open(fname) as f:
            df = pd.read_csv(
                f, sep="\t", header=None,
                usecols=list(GKG_COLS.keys()),
                names=list(GKG_COLS.values()),
                on_bad_lines="skip",
                low_memory=False,
            )
        return df
    except Exception as e:
        log.warning(f"Failed to fetch {url}: {e}")
        return pd.DataFrame()


def parse_tone(tone_csv: str) -> float:
    """Extract overall tone score from GDELT tone field."""
    try:
        return float(str(tone_csv).split(",")[0])
    except:
        return 0.0


def is_relevant(row) -> bool:
    """Keep only articles matching our topic keywords."""
    text = f"{row.get('themes', '')} {row.get('url', '')}".lower()
    return any(kw in text for kw in KEYWORDS)


def collect_bulk(target: int = 15000, max_files: int = 300) -> dict:
    """
    Download GDELT GKG bulk files until we have target articles.
    Each file = 15 min window = ~200-500 relevant articles.
    """
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    articles = []
    seen_urls = set()
    files_tried = 0

    # Build list of recent 15-min file URLs (walk backwards from now)
    now = datetime.utcnow()
    timestamps = []
    t = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
    for _ in range(max_files):
        ts = t.strftime("%Y%m%d%H%M%S")
        timestamps.append(ts)
        t -= timedelta(minutes=15)

    log.info(f"Targeting {target} articles across up to {max_files} GDELT files")

    for ts in timestamps:
        if len(articles) >= target:
            break

        url = f"{GDELT_BASE}{ts}.gkg.csv.zip"
        log.info(f"Fetching {ts} — collected so far: {len(articles)}")

        df = fetch_gdelt_gkg_file(url)
        files_tried += 1

        if df.empty:
            time.sleep(1)
            continue

        # Filter to relevant articles
        df = df[df.apply(is_relevant, axis=1)]
        df = df[df["url"].notna()]

        for _, row in df.iterrows():
            url_val = str(row["url"]).strip()
            if url_val in seen_urls or not url_val.startswith("http"):
                continue
            seen_urls.add(url_val)

            # Parse date
            try:
                date_str = str(row["date"])[:8]
                published = datetime.strptime(date_str, "%Y%m%d").isoformat() + "Z"
            except:
                published = None

            articles.append({
                "url":          url_val,
                "source":       str(row.get("source", "")).strip(),
                "headline":     "",
                "published_at": published,
                "scrape_at":    datetime.utcnow().isoformat() + "Z",
                "body":         "",
                "gdelt_tone":   parse_tone(row.get("tone_csv", "")),
                "themes":       str(row.get("themes", "")),
            })

        time.sleep(0.5)

    log.info(f"Collected {len(articles)} unique relevant URLs from {files_tried} files")

    with open("data/raw/articles_urls.json", "w") as f:
        json.dump(articles, f, indent=2)

    return {"total": len(articles), "files_tried": files_tried}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    result = collect_bulk(target=15000, max_files=300)
    print(result)
