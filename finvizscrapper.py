import requests
from bs4 import BeautifulSoup
import pandas as pd
import trafilatura
import time
import re
import html
from datetime import datetime
import numpy as np
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification


# ---------- Load FinBERT Model (Loads Once) ----------
MODEL_NAME = "ProsusAI/finbert"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)


def get_sentiment_score(text):
    """
    Returns continuous sentiment score between -1 and 1
    Score = P(positive) - P(negative)
    """
    if not text or text.strip() == "":
        return np.nan

    # Truncate for speed
    text = text[:800]

    inputs = tokenizer(
        text,
        return_tensors="pt",
        truncation=True,
        max_length=512
    )

    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.nn.functional.softmax(outputs.logits, dim=1)

    probs = probs.numpy()[0]
    negative, neutral, positive = probs

    score = float(positive - negative)
    return round(score, 4)


def main():
    url = "https://finviz.com/quote.ashx?t=NVDA"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")
    news_table = soup.find("table", class_="fullview-news-outer")

    news = []
    current_date = None
    today_finviz = datetime.today().strftime("%b-%d-%y")

    # ---------- Scrape Finviz News ----------
    for row in news_table.find_all("tr"):
        try:
            cols = row.find_all("td")
            if len(cols) < 2:
                continue

            timestamp_raw = cols[0].text.strip()
            headline_tag = cols[1].find("a")
            if not headline_tag:
                continue

            headline = headline_tag.text.strip()
            link = "https://finviz.com" + headline_tag["href"].strip()

            source_tag = cols[1].find("span", class_="nn")
            source = source_tag.text.strip("()") if source_tag else ""

            # Handle date parsing
            if "Today" in timestamp_raw:
                time_part = timestamp_raw.replace("Today", "").strip()
                current_date = today_finviz
            elif "-" in timestamp_raw:
                parts = timestamp_raw.split(" ")
                current_date = parts[0]
                time_part = parts[1] if len(parts) > 1 else ""
            else:
                time_part = timestamp_raw

            if current_date and time_part:
                try:
                    dt_obj = datetime.strptime(f"{current_date} {time_part}", "%b-%d-%y %I:%M%p")
                    full_timestamp = dt_obj.strftime("%Y-%m-%d %I:%M%p")
                except Exception:
                    full_timestamp = f"{current_date} {time_part}"
            else:
                full_timestamp = timestamp_raw

            news.append([full_timestamp, headline, source, link])

        except Exception as e:
            print("Error parsing row:", e)

    df = pd.DataFrame(news, columns=["timestamp", "headline", "source", "url"])
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['date'] = df['timestamp'].dt.date

    # ---------- Fetch Full Articles ----------
    article_texts = []
    for article_url in df['url']:
        try:
            downloaded = trafilatura.fetch_url(article_url)
            if downloaded:
                article = trafilatura.extract(downloaded)
            else:
                article = ""
        except Exception as e:
            print(f"Error fetching {article_url}: {e}")
            article = ""

        article_texts.append(article)
        time.sleep(1)

    df['stock'] = 'nvidia'
    df['article_text'] = article_texts

    # ---------- Cleaning ----------
    def clean_article_text(text: str) -> str:
        if pd.isna(text) or text.strip() == "":
            return ""

        text = html.unescape(text)
        text = re.sub(r'[|]+', ' ', text)
        text = re.sub(r'<.*?>', ' ', text)
        text = re.sub(r'http\S+|www\.\S+', ' ', text)
        text = text.encode('ascii', 'ignore').decode('ascii')
        text = text.replace('“', '"').replace('”', '"').replace("’", "'").replace('–', '-').replace('—', '-')
        text = re.sub(r'[\*\•\·\▪\◆\▶\-]', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    df['article_text_clean'] = df['article_text'].apply(clean_article_text)

    # ---------- Save + Deduplicate ----------
    file_path = "finviz_data.xlsx"

    try:
        existing_df = pd.read_excel(file_path)
        updated_df = pd.concat([existing_df, df], ignore_index=True)
        clean_df = updated_df.drop_duplicates(subset=["headline"], keep="first")

    except FileNotFoundError:
        clean_df = df

    # ---------- Sentiment Column ----------
    if 'sentiment_score' not in clean_df.columns:
        clean_df['sentiment_score'] = np.nan

    mask = (
        clean_df['article_text_clean'].notna() &
        (clean_df['article_text_clean'] != "") &
        clean_df['sentiment_score'].isna()
    )

    print(f"Calculating sentiment for {mask.sum()} new articles...")

    clean_df.loc[mask, 'sentiment_score'] = (
        clean_df.loc[mask, 'article_text_clean']
        .apply(get_sentiment_score)
    )

    clean_df.to_excel(file_path, index=False)

    print("Excel file updated successfully.")


if __name__ == "__main__":
    main()
