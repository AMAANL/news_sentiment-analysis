import requests
import json
import logging
import os
import time
from datetime import datetime
from newspaper import Article

logger = logging.getLogger(__name__)

class GDELTFetcher:
    def __init__(self):
        self.base_url = "https://api.gdeltproject.org/api/v2/doc/doc"

    def fetch_urls(self, query="economy OR technology OR politics", max_records=250, timespan="24h"):
        """Returns a list of dicts with 'url', 'title', 'domain', 'seendate'"""
        params = {
            "query": query,
            "mode": "artlist",
            "maxrecords": max_records,
            "format": "json",
            "timespan": timespan
        }
        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return data.get("articles", [])
            else:
                logger.error(f"GDELT fetch failed: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"GDELT fetch error: {e}")
            return []


class GNewsFetcher:
    def __init__(self, api_key=None):
        self.api_key = api_key or os.environ.get("GNEWS_API_KEY")
        self.base_url = "https://gnews.io/api/v4/search"

    def fetch_urls(self, query="economy OR technology", max_records=10):
        if not self.api_key:
            logger.warning("No GNews API key provided. Skipping GNews.")
            return []
        
        params = {
            "q": query,
            "lang": "en",
            "max": max_records,
            "apikey": self.api_key
        }
        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return data.get("articles", [])
            else:
                logger.error(f"GNews fetch failed: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"GNews fetch error: {e}")
            return []


def scrape_article_body(url):
    try:
        article = Article(url)
        article.download()
        article.parse()
        return article.text
    except Exception as e:
        logger.debug(f"Failed to scrape body for {url}: {e}")
        return ""


def run(sample_mode=False):
    """
    Run the data collection step.
    If sample_mode is True, fetches very few items for testing.
    """
    raw_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    out_file = os.path.join(raw_dir, "articles.json")
    
    # Load existing articles to avoid re-fetching duplicates
    all_articles = []
    if os.path.exists(out_file):
        try:
            with open(out_file, 'r', encoding='utf-8') as f:
                all_articles = json.load(f)
        except json.JSONDecodeError:
            pass
            
    existing_urls = {a.get('url') for a in all_articles if a.get('url')}
    
    # Initialize fetchers
    gdelt = GDELTFetcher()
    gnews = GNewsFetcher()
    
    # Decide max records
    # To scale to 10k+, one would typically loop this over days or run iteratively.
    # For a single run, max GDELT record default is 250 in API. 
    max_gdelt = 10 if sample_mode else 250
    
    logger.info("Fetching from GDELT...")
    gdelt_articles = gdelt.fetch_urls(max_records=max_gdelt)
    
    logger.info("Fetching from GNews...")
    gnews_articles = gnews.fetch_urls(max_records=10) if gnews.api_key else []
    
    new_articles_metadata = []
    
    for a in gdelt_articles:
        if a.get('url') not in existing_urls:
            new_articles_metadata.append({
                "url": a.get("url"),
                "source": a.get("domain"),
                "headline": a.get("title"),
                "published_at": a.get("seendate"),
                "scrape_at": datetime.utcnow().isoformat()
            })
            existing_urls.add(a.get("url"))
            
    for a in gnews_articles:
        if a.get('url') not in existing_urls:
            new_articles_metadata.append({
                "url": a.get("url"),
                "source": a.get("source", {}).get("name") if isinstance(a.get("source"), dict) else "Unknown",
                "headline": a.get("title"),
                "published_at": a.get("publishedAt"),
                "scrape_at": datetime.utcnow().isoformat()
            })
            existing_urls.add(a.get("url"))
            
    # Download article bodies
    total_new = len(new_articles_metadata)
    logger.info(f"Downloading bodies for {total_new} new articles...")
    
    for i, item in enumerate(new_articles_metadata):
        # Progress log
        if i % 10 == 0 and i > 0:
            logger.info(f"Downloaded {i}/{total_new} bodies...")
            
        body = scrape_article_body(item["url"])
        item["body"] = body
        all_articles.append(item)
        time.sleep(0.1)  # tiny delay to not hammer sites
        
    # Save output
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(all_articles, f, indent=4, ensure_ascii=False)
        
    logger.info(f"Data Collection complete. Saved {len(all_articles)} total articles to {out_file}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run(sample_mode=True)
