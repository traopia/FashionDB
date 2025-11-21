import os
import re
from io import BytesIO

import numpy as np
import pandas as pd
import requests
import torch
from PIL import Image
from transformers import CLIPModel, CLIPProcessor, pipeline

# Constants
EMBEDDINGS_PATH = "data/embeddings/fashion_clip.npy"
URLS_PATH = "data/embeddings/image_urls.npy"
BATCH_SIZE = 50  # flush every 50 images

# Set device: Use GPU if available, otherwise mps if available otherwise CPU
device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"

# Get device directory for local image storage
device_dir = os.getcwd().split('/')[2]

# Load Fashion-CLIP model and processor
model_name = "patrickjohncyh/fashion-clip"
model = CLIPModel.from_pretrained(model_name).to(device)
processor = CLIPProcessor.from_pretrained(model_name)

# Initialize segmentation pipeline
segmenter = pipeline(model="mattmdjaga/segformer_b2_clothes", device=device)


def segment_clothing_white(img, clothes=None):
    """Segment clothing items and set background to white."""
    if clothes is None:
        clothes = ["Background"]
    segments = segmenter(img)

    # Create list of masks
    mask_list = []
    for s in segments:
        if s['label'] in clothes:
            mask_list.append(s['mask'])

    if not mask_list:
        print("No clothing segments found in image.")
        return img  # Return the original image if no segments are found

    # Combine all masks into a single mask
    final_mask = np.array(mask_list[0])
    for mask in mask_list[1:]:
        final_mask = np.maximum(final_mask, np.array(mask))  # Combine masks using max

    # Apply the mask to the image
    img_array = np.array(img)  # Convert image to numpy array
    final_mask = final_mask.astype(bool)  # Convert mask to boolean
    img_array[final_mask] = [255, 255, 255]  # Set unmasked regions to white

    # Convert back to PIL image
    segmented_img = Image.fromarray(img_array)
    return segmented_img


def download_image(image_url):
    """Download an image from a URL, save it locally with the URL as the filename, and return a PIL image."""
    try:
        image = get_image_locally(image_url)
        if image:
            return image
        response = requests.get(image_url, timeout=5)  # 5-second timeout
        response.raise_for_status()  # Raise error for 4xx and 5xx responses
        Image.MAX_IMAGE_PIXELS = 500_000_000
        image = Image.open(BytesIO(response.content)).convert("RGB")

        # Save the image locally with the URL as the filename (sanitized)
        sanitized_filename = image_url.replace("://", "-").replace("/", "_")
        if len(sanitized_filename) > 255:
            return None
        base_dir = f'/Users/{device_dir}/Library/CloudStorage/OneDrive-UvA/fashion_images/images_all'
        image_path = os.path.join(base_dir, sanitized_filename)
        image.save(image_path, format="JPEG")
        print(f"✅ Image saved as {sanitized_filename}")

        return image
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Failed to download {image_url}: {e}")
        return None

def reconstruct_full_vogue_url(url):
    """Insert the missing '/master/w_1280,c_limit/' block if it's a normalized Vogue URL."""
    if "assets.vogue.com/photos/" not in url:
        return url

    try:
        base, rest = url.split("/photos/")
        folder, fname = rest.split("/", 1)
        return f"{base}/photos/{folder}/master/w_1280,c_limit/{fname}"
    except (ValueError, IndexError):
        return url

def get_image_locally(image_url):
    """Retrieve an image from local storage based on matching sanitized filenames."""
    # Also try the reconstructed Vogue URL version
    candidate_urls = [image_url, reconstruct_full_vogue_url(image_url)]

    base_dir = f"/Users/{device_dir}/Library/CloudStorage/OneDrive-UvA/fashion_images/images_all"

    for url in candidate_urls:
        sanitized = url.replace("://", "-").replace("/", "_")
        if len(sanitized) > 255:
            continue

        local_path = os.path.join(base_dir, sanitized)
        if os.path.exists(local_path):
            try:
                Image.MAX_IMAGE_PIXELS = 500_000_000
                img = Image.open(local_path).convert("RGB")
                print(f"✅ Loaded image from {local_path}")
                return img
            except Exception as e:
                print(f"⚠️ Failed loading {local_path}: {e}")

    print("❌ Image not found locally")
    return None


def encode_image(image):
    """Encode image into an embedding."""
    inputs = processor(images=image, return_tensors="pt").to(device)

    with torch.no_grad():
        image_features = model.get_image_features(**inputs).cpu().numpy()
    return image_features


def normalize_vogue_url(url: str) -> str:
    """
    Remove the '/master/w_XXXX[,c_limit]' crop component from Vogue image URLs.
    Works for any width (e.g., w_600, w_800, w_1280).
    """
    # Regex removes: /master/w_### or /master/w_###,c_limit
    return re.sub(r"/master/w_\d+(?:,c_limit)?", "", url)


def load_existing_urls_npy(urls_path=URLS_PATH):
    """
    Load processed URLs and return both the original array and a mapping from normalized URL to index.
    Returns: (urls_array, normalized_url_to_idx)
    """
    if not os.path.exists(urls_path):
        return np.array([], dtype=object), {}

    urls_array = np.load(urls_path, allow_pickle=True)
    normalized_url_to_idx = {}

    for idx, url in enumerate(urls_array):
        url_str = str(url)
        normalized = normalize_vogue_url(url_str)
        # Map normalized URL to index (keep first occurrence if duplicates)
        if normalized not in normalized_url_to_idx:
            normalized_url_to_idx[normalized] = idx

    return urls_array, normalized_url_to_idx


def process_images_parquet(
        OUTPUT_EMB_PATH,
        OUTPUT_URLS_PATH,
        segment=False,
        batch_size=BATCH_SIZE,
        embedding_dim=512
    ):
    """
    Process images from a Parquet file and save embeddings/URLs for this batch only.
    If an image was already processed, copy its stored embedding into the new output files.
    Ensures no duplicate URLs are processed in the output.
    """
    # Load incoming dataframe
    df = pd.read_parquet("https://huggingface.co/datasets/traopia/vogue-runway/resolve/main/vogue_all_data_emb.parquet")
    df = df.groupby("collection").head(11)

    # Load global processed URLs + embeddings
    global_urls, normalized_url_to_idx = load_existing_urls_npy(URLS_PATH)

    if os.path.exists(EMBEDDINGS_PATH):
        global_embs = np.load(EMBEDDINGS_PATH)
    else:
        global_embs = np.empty((0, embedding_dim), dtype=np.float32)

    # Load existing output URLs to avoid duplicates in output files
    output_normalized_urls = set()
    if os.path.exists(OUTPUT_URLS_PATH):
        existing_output_urls = np.load(OUTPUT_URLS_PATH, allow_pickle=True)
        for url in existing_output_urls:
            normalized = normalize_vogue_url(str(url))
            output_normalized_urls.add(normalized)

    # Prepare collectors for this NEW batch
    new_batch_embs = []
    new_batch_urls = []
    
    # Track URLs processed in current batch to avoid duplicates within the same run
    current_batch_normalized_urls = set()
    # Track URLs that failed to download to avoid repeated failed attempts in same run
    failed_urls = set()

    processed_count = 0
    skipped_duplicate_count = 0
    new_count = 0
    failed_count = 0

    for idx, row in df.iterrows():
        image_urls = row["url"]
        if isinstance(image_urls, str):
            image_urls = [image_urls]

        for img_url in image_urls:
            # Normalize URL for matching
            normalized_url = normalize_vogue_url(img_url)

            # Skip if already in current batch (duplicate in same run)
            if normalized_url in current_batch_normalized_urls:
                skipped_duplicate_count += 1
                continue

            # Skip if already in output files
            if normalized_url in output_normalized_urls:
                skipped_duplicate_count += 1
                continue

            # Skip if download failed earlier in this run
            if normalized_url in failed_urls:
                skipped_duplicate_count += 1
                continue

            # Check if URL already processed in global cache using normalized version
            if normalized_url in normalized_url_to_idx:
                # URL already processed → COPY from global cache
                existing_idx = normalized_url_to_idx[normalized_url]
                emb_copy = global_embs[existing_idx]

                new_batch_embs.append(emb_copy)
                new_batch_urls.append(img_url)
                current_batch_normalized_urls.add(normalized_url)
                output_normalized_urls.add(normalized_url)
                processed_count += 1
                continue

            # URL NOT processed → extract now
            image = download_image(img_url)
            if image is None:
                failed_urls.add(normalized_url)
                failed_count += 1
                continue

            if segment:
                image = segment_clothing_white(image)

            embedding = encode_image(image)
            embedding = embedding / torch.linalg.norm(torch.tensor(embedding), ord=2, dim=-1, keepdim=True)
            embedding = embedding.numpy().astype(np.float32).flatten()

            new_batch_embs.append(embedding)
            new_batch_urls.append(img_url)
            current_batch_normalized_urls.add(normalized_url)
            output_normalized_urls.add(normalized_url)
            new_count += 1

            # Also append to global index so future batches skip recomputing
            # Safeguard: only add if not already in global cache
            if normalized_url not in normalized_url_to_idx:
                new_idx = len(global_urls)
                global_urls = np.append(global_urls, img_url)
                global_embs = np.vstack([global_embs, embedding])
                normalized_url_to_idx[normalized_url] = new_idx

            # Batch flush to new output files
            if len(new_batch_embs) >= batch_size:
                flush_to_new_output(new_batch_embs, new_batch_urls, OUTPUT_EMB_PATH, OUTPUT_URLS_PATH)
                new_batch_embs, new_batch_urls = [], []

    # Final flush for remaining items
    if new_batch_embs:
        flush_to_new_output(new_batch_embs, new_batch_urls, OUTPUT_EMB_PATH, OUTPUT_URLS_PATH)

    # Save updated *global* embeddings (cache)
    np.save(EMBEDDINGS_PATH, global_embs)
    np.save(URLS_PATH, global_urls)

    # Verify no duplicates in output
    if os.path.exists(OUTPUT_URLS_PATH):
        output_urls = np.load(OUTPUT_URLS_PATH, allow_pickle=True)
        unique_output_urls = set(normalize_vogue_url(str(url)) for url in output_urls)
        print(f"✅ Finished processing.")
        print(f"   - New embeddings extracted: {new_count}")
        print(f"   - Copied from cache: {processed_count}")
        print(f"   - Skipped duplicates: {skipped_duplicate_count}")
        print(f"   - Failed downloads: {failed_count}")
        print(f"   - Total in output: {len(output_urls)} URLs, {len(unique_output_urls)} unique")
        print(f"   - Total in global cache: {len(global_urls)}")
        
        if len(output_urls) != len(unique_output_urls):
            print(f"⚠️  WARNING: Found {len(output_urls) - len(unique_output_urls)} duplicate URLs in output!")
        else:
            print(f"✅ No duplicates in output - all URLs are unique!")
    else:
        print(f"✅ Finished processing. New: {new_count}, Copied: {processed_count}, Failed: {failed_count}")


def flush_to_new_output(batch_embs, batch_urls, OUTPUT_EMB_PATH, OUTPUT_URLS_PATH):
    """Append a batch of embeddings and URLs to the NEW batch output files."""
    if not batch_embs or not batch_urls:
        return

    # Ensure batch_embs and batch_urls have the same length
    assert len(batch_embs) == len(batch_urls), f"Mismatch: {len(batch_embs)} embeddings vs {len(batch_urls)} URLs"

    # Handle embeddings
    if os.path.exists(OUTPUT_EMB_PATH):
        existing = np.load(OUTPUT_EMB_PATH)
        out_emb = np.vstack([existing, np.stack(batch_embs)])
    else:
        out_emb = np.stack(batch_embs)
    np.save(OUTPUT_EMB_PATH, out_emb)

    # Handle URLs
    if os.path.exists(OUTPUT_URLS_PATH):
        existing = np.load(OUTPUT_URLS_PATH, allow_pickle=True)
        out_urls = np.concatenate([existing, np.array(batch_urls, dtype=object)])
    else:
        out_urls = np.array(batch_urls, dtype=object)
    np.save(OUTPUT_URLS_PATH, out_urls)

    # Verify no duplicates in the output
    unique_normalized = set(normalize_vogue_url(str(url)) for url in out_urls)
    if len(out_urls) != len(unique_normalized):
        print(f"⚠️  WARNING: Found {len(out_urls) - len(unique_normalized)} duplicate URLs after flush!")
    
    print(f"💾 Wrote batch of {len(batch_embs)} embeddings to new output. Total now: {len(out_urls)}")


def main():
    process_images_parquet(
        OUTPUT_EMB_PATH="data/embeddings/fashion_clip_new.npy",
        OUTPUT_URLS_PATH="data/embeddings/image_urls_new.npy",
        segment=True
    )


if __name__ == "__main__":
    main()