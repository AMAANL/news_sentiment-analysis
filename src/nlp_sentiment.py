import json
import logging
import os
import re
from bs4 import BeautifulSoup
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline
import torch

logger = logging.getLogger(__name__)

# Ensure NLTK resources
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)
try:
    nltk.data.find('corpora/wordnet')
except LookupError:
    nltk.download('wordnet', quiet=True)
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)
try:
    nltk.data.find('tokenizers/punkt_tab')
except LookupError:
    nltk.download('punkt_tab', quiet=True)

stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

def clean_text(text):
    if not text:
        return ""
    # Strip HTML
    text = BeautifulSoup(text, "html.parser").get_text()
    # Remove emails/urls
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    text = re.sub(r'\S+@\S+', '', text)
    # Remove boilerplate short lines, extra whitespace
    text = " ".join([word for word in text.split() if len(word) > 1])
    # Lowercase & Tokenize
    tokens = nltk.word_tokenize(text.lower())
    # Remove stopwords and non-alphabetic
    tokens = [lemmatizer.lemmatize(word) for word in tokens if word.isalpha() and word not in stop_words]
    return " ".join(tokens)

def run():
    clean_dir = os.path.join(os.path.dirname(__file__), "..", "data", "cleaned")
    in_file = os.path.join(clean_dir, "articles_cleaned.json")
    out_file = os.path.join(clean_dir, "articles_nlp.json")

    if not os.path.exists(in_file):
        logger.error(f"Input file not found: {in_file}")
        return

    try:
        with open(in_file, 'r', encoding='utf-8') as f:
            articles = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load cleaned articles: {e}")
        return

    logger.info(f"Loaded {len(articles)} articles for NLP processing.")

    # Initialize models
    vader = SentimentIntensityAnalyzer()
    
    if torch.cuda.is_available():
        device = 0
    elif torch.backends.mps.is_available():
        device = "mps"
    else:
        device = -1
    logger.info("Loading Transformer model...")
    # using truncation=True to handle long texts
    sentiment_pipeline = pipeline(
        "sentiment-analysis", 
        model="cardiffnlp/twitter-roberta-base-sentiment", 
        device=device, 
        truncation=True, 
        max_length=512
    )

    processed_articles = []

    for i, article in enumerate(articles):
        if i % 50 == 0 and i > 0:
            logger.info(f"Processed {i}/{len(articles)} articles.")

        body = article.get("body", "")
        headline = article.get("headline", "")

        # 1. Clean Text
        cleaned_body = clean_text(body)
        article["cleaned_body"] = cleaned_body

        # 2. VADER on headline (baseline for news)
        vader_score = vader.polarity_scores(headline)['compound']
        article["vader_score"] = vader_score

        # 3. Transformer on body
        transformer_score = 0.0
        if cleaned_body.strip():
            try:
                res = sentiment_pipeline(body[:2000])[0]
                label = res['label']
                score = res['score']
                # Format specific to cardiffnlp/twitter-roberta-base-sentiment
                if '0' in label:  # Negative
                    transformer_score = -score
                elif '2' in label: # Positive
                    transformer_score = score
                else: # Neutral
                    transformer_score = 0
            except Exception as e:
                logger.debug(f"Transformer failed on article {i}: {e}")
                transformer_score = 0.0

        article["transformer_score"] = float(transformer_score)

        # 4. Composite Score
        # Rule: Use Transformer score if body is substantial, else fallback to VADER headline score
        if len(cleaned_body.split()) >= 20 and abs(transformer_score) > 0.001:
            final_sentiment = transformer_score
        else:
            final_sentiment = vader_score

        article["sentiment_score"] = float(final_sentiment)

        processed_articles.append(article)

    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(processed_articles, f, indent=4, ensure_ascii=False)

    logger.info(f"NLP processing complete. Saved to {out_file}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run()
