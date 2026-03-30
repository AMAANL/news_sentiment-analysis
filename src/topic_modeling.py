import json
import logging
import os
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

def run():
    in_file = os.path.join(os.path.dirname(__file__), "..", "data", "cleaned", "articles_nlp.json")
    out_file = os.path.join(os.path.dirname(__file__), "..", "data", "cleaned", "articles_topic.json")

    if not os.path.exists(in_file):
        logger.error(f"Input file not found: {in_file}")
        return

    with open(in_file, 'r', encoding='utf-8') as f:
        articles = json.load(f)

    logger.info(f"Loaded {len(articles)} articles for Topic Modeling.")

    # Extract text for modeling (using cleaned body or headline if body empty)
    docs = []
    for a in articles:
        text = a.get("cleaned_body", "")
        if not text.strip():
            text = a.get("headline", "")
        docs.append(text)

    # Use a lightweight sentence transformer suitable for BERTopic
    logger.info("Initializing SentenceTransformer & BERTopic...")
    sentence_model = SentenceTransformer("all-MiniLM-L6-v2")
    
    # Optional: configure BERTopic to not remove too many outliers
    topic_model = BERTopic(embedding_model=sentence_model, min_topic_size=2)
    
    if len(docs) < 2:
        logger.warning("Not enough documents for BERTopic. Assigning topic -1.")
        topics = [-1] * len(docs)
    else:
        logger.info("Fitting BERTopic model (this may take a minute)...")
        topics, _ = topic_model.fit_transform(docs)

    # Automatically generate topic labels if possible, else use ID
    try:
        topic_info = topic_model.get_topic_info()
        topic_labels = {}
        for idx, row in topic_info.iterrows():
            tid = row['Topic']
            if tid == -1:
                topic_labels[-1] = "Outlier"
            else:
                words = [w[0] for w in topic_model.get_topic(tid)[:3]]
                topic_labels[tid] = f"Topic_{tid}_" + "_".join(words)
    except Exception as e:
        logger.warning(f"Could not generate advanced topic labels: {e}")
        topic_labels = {t: f"Topic_{t}" for t in set(topics)}

    # Assign
    for i, article in enumerate(articles):
        tid = topics[i]
        article["topic_id"] = int(tid)
        article["topic"] = topic_labels.get(tid, "Unknown")
        
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(articles, f, indent=4, ensure_ascii=False)

    logger.info(f"Topic Modeling complete. Saved to {out_file}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run()
