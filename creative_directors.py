import requests
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import os
import time

def extract_creative_director_from_url(url):
    """Extract creative director (e.g., 'Alessandro Michele') from a Vogue show URL."""
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            print(f"❌ Failed to retrieve {url} (status {r.status_code})")
            return url, None

        soup = BeautifulSoup(r.content, 'html.parser')

        # Find text like 'By Alessandro Michele'
        byline_tag = soup.find(string=re.compile(r'\bBy\s+[A-Z][a-z]+'))
        if byline_tag:
            match = re.search(r'\bBy\s+(.*)', byline_tag.strip())
            if match:
                creative_director = match.group(1).strip()
                creative_director = re.sub(r'[.,;]+$', '', creative_director)

                # Filter out junk: too long or looks like JSON
                if len(creative_director) > 100 or any(ch in creative_director for ch in ['{', '}', ':', '"']):
                    return url, None

                return url, creative_director

    except Exception as e:
        print(f"⚠️ Error scraping {url}: {e}")

    return url, None


def scrape_creative_directors_parallel(urls, csv_path="creative_directors.csv", max_workers=10):
    """
    Scrape creative directors from multiple Vogue show URLs in parallel.
    Saves results incrementally to CSV, so progress is not lost if blocked or interrupted.
    """
    # Load already saved results
    if os.path.exists(csv_path):
        existing_df = pd.read_csv(csv_path)
        done_urls = set(existing_df["url"].tolist())
        print(f"🔄 Resuming: {len(done_urls)} URLs already done.")
    else:
        existing_df = pd.DataFrame(columns=["url", "creative_director"])
        done_urls = set()

    remaining_urls = [u for u in urls if u not in done_urls]
    print(f"🕵️ Starting scraping for {len(remaining_urls)} remaining URLs...")

    if not remaining_urls:
        print("✅ All URLs already scraped.")
        return existing_df

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(extract_creative_director_from_url, url): url for url in remaining_urls}

        for i, future in enumerate(as_completed(future_to_url), 1):
            url, director = future.result()
            print(f"[{i}/{len(remaining_urls)}] ✅ {url} → {director}")

            # Append to dataframe and save immediately
            new_row = pd.DataFrame([{"url": url, "creative_director": director}])
            existing_df = pd.concat([existing_df, new_row], ignore_index=True)
            existing_df.to_csv(csv_path, index=False)
            time.sleep(0.5)

    print(f"\n💾 All results saved to {csv_path}")
    return existing_df

# Example usage:
if __name__ == "__main__":
    df_url = "https://huggingface.co/datasets/traopia/FashionDB/resolve/main/data_vogue_final.parquet"
    df = pd.read_parquet(df_url)
    df = df.drop_duplicates(subset=["URL"])
    urls = df["URL"].tolist()
    result = scrape_creative_directors_parallel(urls, csv_path="creative_directors.csv", max_workers=10)
