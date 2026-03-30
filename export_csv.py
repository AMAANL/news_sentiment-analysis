import sqlite3
import pandas as pd
from pathlib import Path

Path("data/export").mkdir(parents=True, exist_ok=True)

conn = sqlite3.connect("data/db/app.db")

for table in ["articles", "daily_sentiment_aggregation", "events", "event_analysis"]:
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    out = f"data/export/{table}.csv"
    df.to_csv(out, index=False)
    print(f"Saved {len(df)} rows → {out}")

conn.close()
print("\nDone. Upload the 4 files in data/export/ to Power BI.")