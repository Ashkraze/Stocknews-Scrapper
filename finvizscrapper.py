import requests
from bs4 import BeautifulSoup
import pandas as pd
import trafilatura
import time
import re
import html
from datetime import datetime


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

            # Handling "Today" case as my data has Today instead of date
            if "Today" in timestamp_raw:
                time_part = timestamp_raw.replace("Today", "").strip()
                current_date = today_finviz
            elif "-" in timestamp_raw: 
                parts = timestamp_raw.split(" ")
                current_date = parts[0]
                time_part = parts[1] if len(parts) > 1 else ""
            else:  # Just time
                time_part = timestamp_raw

            # Combine date and time into full timestamp
            if current_date and time_part:
                try:
                    dt_obj = datetime.strptime(f"{current_date} {time_part}", "%b-%d-%y %I:%M%p")
                    full_timestamp = dt_obj.strftime("%Y-%m-%d %I:%M%p")
                except Exception:
                    full_timestamp = f"{current_date} {time_part}"  # fallback
            else:
                full_timestamp = timestamp_raw

            news.append([full_timestamp, headline, source, link])
        except Exception as e:
            print("Error parsing row:", e)

    df = pd.DataFrame(news, columns=["timestamp", "headline", "source", "url"])
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['date'] = df['timestamp'].dt.date

    # ---------- Fetch full articles ----------
    article_texts = []
    for url in df['url']:
        try:
            downloaded = trafilatura.fetch_url(url)
            if downloaded:
                article = trafilatura.extract(downloaded)
            else:
                article = ""
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            article = ""

        article_texts.append(article)
        time.sleep(1)  

    df['stock'] = 'nvidia'
    df['article_text'] = article_texts

    # ---------- Cleaning ----------
    def clean_article_text(text: str) -> str:
        if pd.isna(text) or text.strip() == "":
            return ""

        text = html.unescape(text)  # Unescape HTML
        text = re.sub(r'[|]+', ' ', text)  # Pipes
        text = re.sub(r'<.*?>', ' ', text)  # HTML tags
        text = re.sub(r'http\S+|www\.\S+', ' ', text)  # URLs
        text = text.encode('ascii', 'ignore').decode('ascii')  # Non-ascii
        text = text.replace('“', '"').replace('”', '"').replace("’", "'").replace('–', '-').replace('—', '-')
        text = re.sub(r'[\*\•\·\▪\◆\▶\-]', ' ', text)  # Bullets
        text = re.sub(r'\s+', ' ', text)  # Extra whitespace
        return text.strip()

    df['article_text_clean'] = df['article_text'].apply(clean_article_text)

    # ---------- Save to Excel ----------
    file_path = "finviz_data.xlsx"
    try:
        existing_df = pd.read_excel(file_path)
        updated_df = pd.concat([existing_df, df], ignore_index=True)
        
    except FileNotFoundError:
        updated_df = df
        clean_df = updated_df.drop_duplicates(keep = "first")
    clean_df.to_excel(file_path, index=False)


if __name__ == "__main__":
    main()

