import sqlite3
import pandas as pd
from pathlib import Path
from scipy import stats

Path("data/export").mkdir(parents=True, exist_ok=True)

conn = sqlite3.connect("data/db/app.db")

# ── Export the 3 tables that already exist ────────────────────
for table in ["articles", "daily_sentiment_aggregation", "events"]:
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    df.to_csv(f"data/export/{table}.csv", index=False)
    print(f"Saved {len(df)} rows → data/export/{table}.csv")

# ── Build event_analysis from scratch ────────────────────────
articles_df = pd.read_sql("SELECT * FROM articles", conn)
events_df   = pd.read_sql("SELECT * FROM events",   conn)

# Normalise the date column — handle both ISO and plain YYYY-MM-DD
articles_df["published_at"] = pd.to_datetime(
    articles_df["published_at"], utc=True, errors="coerce"
)

WINDOW = 7
results = []

for _, ev in events_df.iterrows():
    # Try both common column names the antigravity pipeline might have used
    date_col = "event_date" if "event_date" in ev.index else "date"
    pivot = pd.Timestamp(ev[date_col], tz="UTC")

    before = articles_df[
        (articles_df["published_at"] >= pivot - pd.Timedelta(days=WINDOW)) &
        (articles_df["published_at"] <  pivot)
    ]["sentiment_score"].dropna()

    after = articles_df[
        (articles_df["published_at"] >= pivot) &
        (articles_df["published_at"] <  pivot + pd.Timedelta(days=WINDOW))
    ]["sentiment_score"].dropna()

    if len(before) >= 2 and len(after) >= 2:
        t_stat, p_value = stats.ttest_ind(before, after)
    else:
        t_stat, p_value = None, None

    results.append({
        "event_name":   ev.get("event_name", ev.get("name", "")),
        "event_date":   str(ev[date_col]),
        "before_mean":  round(before.mean(), 4) if len(before) else None,
        "after_mean":   round(after.mean(),  4) if len(after)  else None,
        "before_count": len(before),
        "after_count":  len(after),
        "t_statistic":  round(t_stat,  4) if t_stat  else None,
        "p_value":      round(p_value, 4) if p_value else None,
        "significant":  bool(p_value < 0.05) if p_value else False,
    })

event_analysis_df = pd.DataFrame(results)

# Save to SQLite and CSV
event_analysis_df.to_sql("event_analysis", conn, if_exists="replace", index=False)
event_analysis_df.to_csv("data/export/event_analysis.csv", index=False)
print(f"Saved {len(event_analysis_df)} rows → data/export/event_analysis.csv")

conn.close()

print("\nAll 4 CSVs ready in data/export/")
print("\nEvent analysis results:")
print(event_analysis_df[["event_name","before_mean","after_mean","p_value","significant"]].to_string(index=False))