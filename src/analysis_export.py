import json
import logging
import os
import sqlite3
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

def define_events():
    # Placeholder events representing real-world anchors
    return [
        {"event_name": "US Elections Nov", "event_date": "2024-11-05"},
        {"event_name": "Fed Rate Cut Sept", "event_date": "2024-09-18"},
        {"event_name": "Tech Stock Crash August", "event_date": "2024-08-05"}
    ]

def run():
    in_file = os.path.join(os.path.dirname(__file__), "..", "data", "cleaned", "articles_topic.json")
    db_dir = os.path.join(os.path.dirname(__file__), "..", "data", "db")
    os.makedirs(db_dir, exist_ok=True)
    db_file = os.path.join(db_dir, "app.db")

    if not os.path.exists(in_file):
        logger.error(f"Input file not found: {in_file}")
        return

    logger.info("Loading articles into Pandas DataFrame...")
    with open(in_file, 'r', encoding='utf-8') as f:
        articles = json.load(f)

    df = pd.DataFrame(articles)
    
    if df.empty:
        logger.warning("DataFrame is empty. Nothing to export.")
        return

    # 1. Validation Assertions
    # Drop rows without required data to ensure integrity
    initial_len = len(df)
    df = df.dropna(subset=['sentiment_score', 'topic', 'published_at'])
    if len(df) < initial_len:
        logger.warning(f"Dropped {initial_len - len(df)} rows due to NULLs in critical columns.")

    # Convert published_at to purely dates for aggregation
    df['published_date'] = pd.to_datetime(df['published_at']).dt.date

    # 2. Daily Sentiment Aggregation
    logger.info("Generating daily sentiment aggregations...")
    agg_df = df.groupby(['topic', 'published_date']).agg(
        avg_sentiment=('sentiment_score', 'mean'),
        sentiment_std=('sentiment_score', 'std'),
        article_count=('url', 'count')
    ).reset_index()

    # Fill NaNs in std with 0 (for days with 1 article)
    agg_df['sentiment_std'] = agg_df['sentiment_std'].fillna(0)

    # Convert date to string for SQLite compatibility
    agg_df['published_date'] = agg_df['published_date'].astype(str)
    
    # 3. Events Table & T-Test Analysis
    events = define_events()
    logger.info("Calculating event comparison metrics (T-Tests)...")
    
    event_results = []
    
    # We will compute shifting metrics 7 days before and after
    df['date'] = pd.to_datetime(df['published_at']).dt.date
    
    for ev in events:
        ev_date = pd.to_datetime(ev['event_date']).date()
        
        pre_mask = (df['date'] >= (ev_date - pd.Timedelta(days=7))) & (df['date'] < ev_date)
        post_mask = (df['date'] > ev_date) & (df['date'] <= (ev_date + pd.Timedelta(days=7)))
        
        pre_scores = df[pre_mask]['sentiment_score'].dropna()
        post_scores = df[post_mask]['sentiment_score'].dropna()
        
        if len(pre_scores) > 2 and len(post_scores) > 2:
            t_stat, p_val = stats.ttest_ind(pre_scores, post_scores, equal_var=False)
            mean_diff = post_scores.mean() - pre_scores.mean()
        else:
            t_stat, p_val, mean_diff = None, None, None
            
        event_results.append({
            "event_name": ev['event_name'],
            "event_date": ev['event_date'],
            "pre_7d_volume": len(pre_scores),
            "post_7d_volume": len(post_scores),
            "sentiment_shift_mean": mean_diff,
            "t_stat": t_stat,
            "p_value": p_val
        })
        
    events_results_df = pd.DataFrame(event_results)

    # 4. Export to SQLite
    logger.info(f"Exporting to database: {db_file}...")
    conn = sqlite3.connect(db_file)
    
    export_cols = ['url', 'source', 'headline', 'published_at', 'topic', 'sentiment_score', 'vader_score', 'transformer_score']
    exist_cols = [c for c in export_cols if c in df.columns]
    
    df[exist_cols].to_sql('articles', conn, if_exists='replace', index=False)
    agg_df.to_sql('daily_sentiment_aggregation', conn, if_exists='replace', index=False)
    events_results_df.to_sql('events', conn, if_exists='replace', index=False)
    
    conn.close()
    
    logger.info("Export completed successfully. Database is ready for Power BI.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run()
