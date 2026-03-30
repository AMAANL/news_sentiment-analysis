import json
import time
import logging
from newspaper import Article
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

log = logging.getLogger(__name__)


def _scrape_one(art):
    """Scrape a single article. Returns the article dict or None on failure."""
    try:
        a = Article(art["url"], fetch_images=False)
        a.download()
        a.parse()
        art["headline"] = a.title or ""
        art["body"]     = a.text  or ""
        if len(art["body"]) > 100:
            return art
    except Exception:
        pass
    return None


def scrape_bodies(input_path="data/raw/articles_urls.json",
                  output_path="data/raw/articles.json",
                  max_articles=15000,
                  delay=0.2,
                  workers=10):
    """
    Read URL list from GDELT bulk step, download full article bodies
    using newspaper3k with concurrent threads, and save the result.
    """
    with open(input_path) as f:
        articles = json.load(f)

    articles = articles[:max_articles]
    scraped = []
    failed = 0

    log.info(f"Starting body scrape of {len(articles)} articles with {workers} workers...")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_scrape_one, art): i for i, art in enumerate(articles)}
        for future in as_completed(futures):
            idx = futures[future]
            result = future.result()
            if result:
                scraped.append(result)
            else:
                failed += 1

            total_done = len(scraped) + failed
            if total_done % 200 == 0:
                log.info(f"Progress: {total_done}/{len(articles)} — scraped: {len(scraped)} — failed: {failed}")

    log.info(f"Scraped {len(scraped)} articles with body text ({failed} failed)")

    with open(output_path, "w") as f:
        json.dump(scraped, f, indent=2)

    return {"scraped": len(scraped), "failed": failed}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    result = scrape_bodies()
    print(result)
