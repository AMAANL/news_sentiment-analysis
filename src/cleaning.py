import json
import logging
import os
from urllib.parse import urlparse, urlunparse
from dateutil import parser as date_parser
from datetime import timezone
from datasketch import MinHash, MinHashLSH
import copy

logger = logging.getLogger(__name__)

def normalize_url(url):
    """Normalize URL by removing query parameters and fragments."""
    try:
        parsed = urlparse(url)
        # Reconstruct without query and fragment
        clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
        return clean_url.rstrip('/')
    except Exception:
        return url

def normalize_date(date_str):
    """Parse string to UTC datetime ISO format."""
    try:
        dt = date_parser.parse(date_str)
        # Convert to UTC if timezone aware, else assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.isoformat()
    except Exception:
        return None

def get_minhash(text, num_perm=128):
    """Generate MinHash for a given text."""
    m = MinHash(num_perm=num_perm)
    for word in text.split():
        m.update(word.encode('utf8'))
    return m

def run():
    raw_file = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "articles.json")
    clean_dir = os.path.join(os.path.dirname(__file__), "..", "data", "cleaned")
    os.makedirs(clean_dir, exist_ok=True)
    out_file = os.path.join(clean_dir, "articles_cleaned.json")
    report_file = os.path.join(clean_dir, "data_quality_report.txt")

    if not os.path.exists(raw_file):
        logger.error(f"Raw data file not found: {raw_file}. Please run collection first.")
        return

    try:
        with open(raw_file, 'r', encoding='utf-8') as f:
            articles = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load raw articles: {e}")
        return

    initial_count = len(articles)
    logger.info(f"Loaded {initial_count} raw articles.")

    # 1. URL Deduplication & Date Normalization
    url_seen = set()
    first_pass = []
    
    invalid_dates_count = 0
    short_body_count = 0

    for idx, article in enumerate(articles):
        # URL
        raw_url = article.get("url", "")
        clean_url = normalize_url(raw_url)
        if not clean_url or clean_url in url_seen:
            continue
            
        url_seen.add(clean_url)
        article["url"] = clean_url

        # Date
        raw_date = article.get("published_at")
        if raw_date:
            norm_date = normalize_date(raw_date)
            if norm_date:
                article["published_at"] = norm_date
            else:
                invalid_dates_count += 1
                article["published_at"] = None
        else:
            invalid_dates_count += 1

        # Body length check
        body = article.get("body", "")
        if len(body.split()) < 50:
            short_body_count += 1
            # We don't drop them here to preserve data richness, but log it.
            
        first_pass.append(article)

    # 2. Body Deduplication with MinHash LSH
    logger.info(f"Running MinHash deduplication on {len(first_pass)} articles...")
    lsh = MinHashLSH(threshold=0.8, num_perm=128)
    
    final_articles = []
    minhash_dupes = 0
    
    for item in first_pass:
        body = item.get("body", "")
        if not body:
            continue
            
        m = get_minhash(body)
        result = lsh.query(m)
        if not result:
            # Not a duplicate
            doc_id = f"doc_{len(final_articles)}"
            lsh.insert(doc_id, m)
            final_articles.append(item)
        else:
            minhash_dupes += 1

    final_count = len(final_articles)
    
    # Generate Report
    report = [
        "Data Quality Report",
        "===================",
        f"Initial Articles: {initial_count}",
        f"Exact URL Duplicates Removed: {initial_count - len(first_pass)}",
        f"MinHash Body Duplicates Removed: {minhash_dupes}",
        f"Invalid/Unparseable Dates: {invalid_dates_count}",
        f"Articles with <50 Words: {short_body_count}",
        f"Final Cleaned Articles: {final_count}",
        "==================="
    ]
    
    report_str = "\n".join(report)
    logger.info(f"\n{report_str}")
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report_str)

    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(final_articles, f, indent=4, ensure_ascii=False)
        
    logger.info(f"Saved cleaned data to {out_file}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run()
