import logging
import argparse
import sys
import os

# Ensure the root directory is in the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    parser = argparse.ArgumentParser(description="News Sentiment Intelligence Pipeline")
    parser.add_argument("--collect", action="store_true", help="Run data collection")
    parser.add_argument("--clean", action="store_true", help="Run data cleaning")
    parser.add_argument("--nlp", action="store_true", help="Run NLP & sentiment analysis")
    parser.add_argument("--topic", action="store_true", help="Run topic modeling")
    parser.add_argument("--export", action="store_true", help="Run event analysis & DB export")
    parser.add_argument("--all", action="store_true", help="Run the entire pipeline")
    
    args = parser.parse_args()

    if args.all:
        args.collect = args.clean = args.nlp = args.topic = args.export = True

    if not any([args.collect, args.clean, args.nlp, args.topic, args.export, args.all]):
        logging.info("No stages selected. Use --help to see available options.")
        return

    try:
        if args.collect:
            logging.info("Phase 1: GDELT Bulk URL Collection...")
            from src.collection_gdelt_bulk import collect_bulk
            result = collect_bulk(target=15000, max_files=300)
            logging.info(f"Phase 1 complete: {result['total']} URLs from {result['files_tried']} files.")

            logging.info("Phase 2: Scraping article bodies (this will take a while)...")
            from src.scrape_bodies import scrape_bodies
            scrape_result = scrape_bodies(max_articles=15000, delay=0.2)
            logging.info(f"Phase 2 complete: {scrape_result['scraped']} articles scraped ({scrape_result['failed']} failed).")
        
        if args.clean:
            logging.info("Starting Data Cleaning & Deduplication...")
            from src import cleaning; cleaning.run()
            logging.info("Data Cleaning complete.")

        if args.nlp:
            logging.info("Starting NLP & Sentiment Analysis...")
            from src import nlp_sentiment; nlp_sentiment.run()
            logging.info("NLP & Sentiment Analysis complete.")

        if args.topic:
            logging.info("Starting Topic Modeling...")
            from src import topic_modeling; topic_modeling.run()
            logging.info("Topic Modeling complete.")

        if args.export:
            logging.info("Starting Event Analysis & Database Export...")
            from src import analysis_export; analysis_export.run()
            logging.info("Event Analysis & Export complete.")

        logging.info("Pipeline Execution Successfully Completed.")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
