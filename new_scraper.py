import requests
import json
import re
from bs4 import BeautifulSoup
from unidecode import unidecode
import pandas as pd
import os
import sys
from typing import List, Dict, Optional
import time

# Configuration
BATCH_SIZE = 50  # Save after every N rows
OUTPUT_PATH = "data/data_vogue_final_with_images.parquet"


def modify_image_url(original_url):
    """Replace the width parameter in the URL for a higher-resolution image."""
    if original_url:
        return original_url.replace("w_360", "w_1280")
    return original_url


def scrape_collection_images(url: str, timeout: int = 10) -> List[Dict[str, str]]:
    """
    Scrape image data from a Vogue collection URL.
    
    Args:
        url: URL to scrape
        timeout: Request timeout in seconds
        
    Returns:
        List of dictionaries with 'url' and 'alt_text' keys
    """
    if not url or pd.isna(url):
        return []
    
    try:
        # Send request
        r = requests.get(url, timeout=timeout)
        if r.status_code != 200:
            print(f"⚠️  Failed to retrieve {url} - Status code: {r.status_code}")
            return []

        # Parse the page content
        soup = BeautifulSoup(r.content, 'html.parser')

        # Find script tag with runwayShowGalleries
        script_tag = soup.find("script", string=re.compile(r'"runwayShowGalleries":'))
        script_content = script_tag.string if script_tag else None

        if not script_content:
            print(f"⚠️  No runwayShowGalleries script found in {url}")
            return []

        # Extract JSON data
        json_data_match = re.search(r'"runwayShowGalleries":\s*({.*?})\s*;', script_content, re.DOTALL)
        if not json_data_match:
            print(f"⚠️  Could not extract JSON data from {url}")
            return []

        json_data_str = json_data_match.group(1).replace("\\u002F", "/")
        json_decoder = json.JSONDecoder()
        json_data, _ = json_decoder.raw_decode(json_data_str)

        galleries = json_data.get("galleries", [])

        # Filter only gallery with title == "Collection"
        collection_gallery = [g for g in galleries if g.get("title") == "Collection"]

        images_data = []
        if collection_gallery:
            gallery = collection_gallery[0]  # should be only one
            for item in gallery.get("items", []):
                image_info = item.get("image", {})
                src = image_info.get("sources", {}).get("sm", {}).get("url")
                alt_text = image_info.get("altText")

                # Remove "Image may contain" if present
                if alt_text and alt_text.lower().startswith("image may contain"):
                    alt_text = alt_text[len("Image may contain"):].strip()

                if src:
                    images_data.append({
                        "url": modify_image_url(src),
                        "alt_text": alt_text or "",
                    })
        else:
            print(f"⚠️  No Collection gallery found in {url}")

        return images_data

    except requests.exceptions.RequestException as e:
        print(f"⚠️  Request error for {url}: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"⚠️  JSON decode error for {url}: {e}")
        return []
    except Exception as e:
        print(f"⚠️  Unexpected error for {url}: {e}")
        return []


def is_processed(image_data) -> bool:
    """Check if image_data indicates the row has been processed."""
    # Handle None explicitly
    if image_data is None:
        return False
    
    # Handle pandas NA values safely
    try:
        if pd.isna(image_data):
            return False
    except (TypeError, ValueError):
        # If pd.isna fails, it's likely not a pandas NA value, continue
        pass
    
    # If it's a list (even empty), it means we attempted to scrape it
    if isinstance(image_data, list):
        return True
    
    # If it's any other non-None, non-NA value, consider it processed
    return True


def save_batch(df: pd.DataFrame, output_path: str):
    """Save dataframe to parquet file, creating directory if needed."""
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_path)
    if output_dir:  # Only create directory if path contains a directory
        os.makedirs(output_dir, exist_ok=True)
    df.to_parquet(output_path, index=False)


def main():
    """Load dataframe and scrape image data for each collection URL."""
    # Load source dataframe
    df_url = "https://huggingface.co/datasets/traopia/FashionDB/resolve/main/data_vogue_final.parquet"
    print(f"Loading dataframe from {df_url}...")
    df_source = pd.read_parquet(df_url)
    print(f"✅ Loaded {len(df_source)} rows from source")
    
    # Check if 'URL' or 'collection' column exists (URL contains the collection page URL to scrape)
    url_column = None
    for col in ["URL", "collection", "url"]:
        if col in df_source.columns:
            url_column = col
            break
    
    if url_column is None:
        print("❌ Error: No URL column found in dataframe (looking for 'URL', 'collection', or 'url')")
        print(f"Available columns: {df_source.columns.tolist()}")
        return
    
    print(f"   Using '{url_column}' column for collection URLs")
    
    # Load existing results if available, otherwise start fresh
    if os.path.exists(OUTPUT_PATH):
        print(f"📂 Loading existing results from {OUTPUT_PATH}...")
        try:
            df_existing = pd.read_parquet(OUTPUT_PATH)
            print(f"✅ Loaded {len(df_existing)} rows from existing file")
            
            # Determine the best unique identifier for merging
            # Try common unique identifier columns
            unique_id_col = None
            for col in ["URL", "url", "id", "index"]:
                if col in df_source.columns and col in df_existing.columns:
                    unique_id_col = col
                    break
            
            if unique_id_col:
                # Merge on unique identifier
                print(f"   Using '{unique_id_col}' as unique identifier for merging")
                df = df_source.merge(
                    df_existing[[unique_id_col, "image_data"]], 
                    on=unique_id_col, 
                    how="left",
                    suffixes=("", "_existing")
                )
                # Use existing image_data if available, otherwise keep None
                if "image_data_existing" in df.columns:
                    df["image_data"] = df["image_data_existing"].where(
                        pd.notna(df["image_data_existing"]), 
                        df.get("image_data", None)
                    )
                    df = df.drop(columns=["image_data_existing"], errors="ignore")
            else:
                # Fallback: merge on index if no unique identifier found
                print("   No unique identifier found, using index for merging")
                if len(df_source) == len(df_existing):
                    df = df_source.copy()
                    df["image_data"] = df_existing["image_data"].values
                else:
                    print("   ⚠️  Row count mismatch, starting fresh")
                    df = df_source.copy()
                    df["image_data"] = None
                    
        except Exception as e:
            print(f"⚠️  Error loading existing file: {e}. Starting fresh.")
            df = df_source.copy()
            df["image_data"] = None
    else:
        print("📝 Starting fresh - no existing results found")
        df = df_source.copy()
        df["image_data"] = None
    
    # Ensure image_data column exists
    if "image_data" not in df.columns:
        df["image_data"] = None
    
    # Find rows that need processing
    rows_to_process = []
    for idx, row in df.iterrows():
        image_data = row.get("image_data")
        if not is_processed(image_data):
            rows_to_process.append(idx)
    
    total_rows = len(df)
    rows_to_process_count = len(rows_to_process)
    already_processed = total_rows - rows_to_process_count
    
    print(f"\n📊 Processing status:")
    print(f"   - Total rows: {total_rows}")
    print(f"   - Already processed: {already_processed}")
    print(f"   - Remaining to process: {rows_to_process_count}")
    print(f"   - Batch size: {BATCH_SIZE}")
    print(f"   - Output path: {OUTPUT_PATH}\n")
    
    if rows_to_process_count == 0:
        print("✅ All rows already processed!")
        return
    
    # Process rows in batches
    processed_in_this_run = 0
    batch_count = 0
    
    for i, idx in enumerate(rows_to_process):
        row = df.iloc[idx]
        collection_url = row.get(url_column)
        
        # Scrape images
        images_data = scrape_collection_images(collection_url)
        
        # Store image data (empty list if no images found)
        df.at[idx, "image_data"] = images_data if images_data else []
        processed_in_this_run += 1
        
        # Save after every batch
        if (i + 1) % BATCH_SIZE == 0 or i == len(rows_to_process) - 1:
            batch_count += 1
            print(f"💾 Saving batch {batch_count} ({processed_in_this_run} rows processed in this run)...")
            save_batch(df, OUTPUT_PATH)
            print(f"✅ Saved! Progress: {already_processed + processed_in_this_run}/{total_rows} total rows processed")
        
        # Progress update
        if (i + 1) % 10 == 0 or i == len(rows_to_process) - 1:
            progress_pct = ((i + 1) / rows_to_process_count) * 100
            print(f"Progress: {i + 1}/{rows_to_process_count} remaining ({progress_pct:.1f}%)")
        
        # Small delay to be respectful to the server
        time.sleep(0.5)
    
    # Final save
    print(f"\n💾 Final save...")
    save_batch(df, OUTPUT_PATH)
    
    # Print summary
    rows_with_images = df["image_data"].apply(
        lambda x: len(x) > 0 if isinstance(x, list) else False
    ).sum()
    
    print(f"\n✅ Done!")
    print(f"   - Total rows: {total_rows}")
    print(f"   - Rows with images: {rows_with_images}")
    print(f"   - Rows processed in this run: {processed_in_this_run}")
    print(f"   - Output saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()