
import torch
import requests
import json
from io import BytesIO
from PIL import Image
from transformers import CLIPProcessor, CLIPModel, pipeline
import os
import numpy as np
import torch
import pandas as pd
import timm
import torchvision.transforms as T




#set device: Use GPU if availanle, otherwise mps if available otherwise CPU
device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
BATCH_SIZE = 50  # flush every 50 images


device_dir = os.getcwd().split('/')[2]
# Initialize segmentation pipeline
segmenter = pipeline(model="mattmdjaga/segformer_b2_clothes", device = device)


def segment_clothing_white(img, clothes=["Background"]):
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
    img_array[final_mask] = [255,255,255]  # Set unmasked regions to black

    # Convert back to PIL image
    segmented_img = Image.fromarray(img_array)
    return segmented_img



# import re

# pattern = re.compile(r"(beauty|detail)", re.IGNORECASE)

# def clean_row(row):
#     original = row["image_urls_sample"]

#     # Clean pool: valid URLs from image_urls
#     clean_pool = [
#         u for u in row["image_urls"]
#         if isinstance(u, str) and not pattern.search(u)
#     ]

#     # If no clean option exists, return unchanged
#     if not clean_pool:
#         return original

#     # Index in pool for deterministic assignment
#     pool_idx = 0
#     cleaned_list = []

#     for url in original:
#         if isinstance(url, str) and pattern.search(url):
#             # Bad → replace with next clean URL
#             if pool_idx < len(clean_pool):
#                 cleaned_list.append(clean_pool[pool_idx])
#                 pool_idx += 1
#             else:
#                 # not enough clean URLs → fallback to original bad one
#                 cleaned_list.append(url)
#         else:
#             # Good → keep
#             cleaned_list.append(url)

#     return cleaned_list

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
        if len(sanitized_filename)> 255:
            return None
        image.save(f'/Users/{device_dir}/Library/CloudStorage/OneDrive-UvA/fashion_images/images_all/'+sanitized_filename, format="JPEG")
        print(f"✅ Image saved as {sanitized_filename}")
        
        return image
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Failed to download {image_url}: {e}")
        return None


def get_image_locally(image_url):
    """Retrieve an image from local storage based on its sanitized filename."""
    sanitized_filename = image_url.replace("://", "-").replace("/", "_")
    if len(sanitized_filename) > 255:
        return None

    local_path = f'/Users/{device_dir}/Library/CloudStorage/OneDrive-UvA/fashion_images/images_all/{sanitized_filename}'
    
    if os.path.exists(local_path):
        try:
            Image.MAX_IMAGE_PIXELS = 500_000_000
            image = Image.open(local_path).convert("RGB")
            print(f"✅ Image loaded from {local_path}")
            return image
        except Exception as e:
            print(f"⚠️ Failed to load image from {local_path}: {e}")
            return None
    if os.path.exists(f'/Users/{device_dir}/Library/CloudStorage/OneDrive-UvA/fashion_images/{sanitized_filename}'):
        try:
            Image.MAX_IMAGE_PIXELS = 500_000_000
            image = Image.open(local_path).convert("RGB")
            print(f"✅ Image loaded from fashion_image")
            return image
        except Exception as e:
            print(f"⚠️ Failed to load image from {local_path}: {e}")
            return None
    if os.path.exists(f'/Users/{device_dir}/Library/CloudStorage/OneDrive-UvA/fashion_images/images/{sanitized_filename}'):
        try:
            Image.MAX_IMAGE_PIXELS = 500_000_000
            image = Image.open(local_path).convert("RGB")
            print(f"✅ Image loaded from images")
            return image
        except Exception as e:
            print(f"⚠️ Failed to load image from {local_path}: {e}")
            return None
    else:
        print(f"⚠️ Image not found locally at {local_path}")
        return None



def encode_image(image):
    """Encode image into an embedding."""
    inputs = processor(images=image, return_tensors="pt").to(device)

    with torch.no_grad():
        embedding = model.get_image_features(**inputs).cpu().numpy()  # Move to CPU for stability

    embedding /= torch.linalg.norm(torch.tensor(embedding), ord=2, dim=-1, keepdim=True)
    embedding = embedding.numpy().astype(np.float32).flatten()
    return embedding




def encode_image_vit(image):
    """Encode an image with timm ResNet or ViT backbone."""
    transform = T.Compose([
    T.Resize((224, 224)),
    T.ToTensor(),
    T.Normalize(mean=(0.485, 0.456, 0.406), std=(0.229, 0.224, 0.225))])
    image_tensor = transform(image).unsqueeze(0).to(device)
    with torch.no_grad():
        features = model(image_tensor)
    embedding = features.cpu().numpy().flatten().astype(np.float32)
    embedding /= np.linalg.norm(embedding) + 1e-8  # normalize
    return embedding


def load_existing_urls_npy(urls_path):
    """Load already processed image URLs from .npy file."""
    if not os.path.exists(urls_path):
        return set()
    urls_array = np.load(urls_path, allow_pickle=True)
    return set(urls_array)



def process_image_parquet_replaced(parquet_path, extractor,segment=True, batch_size=BATCH_SIZE):
    df = pd.read_parquet("data/data_vogue_final_with_images_sampled.parquet")
    df = df[df["category"]=="ready-to-wear"]

    # 1. Collect replacement URLs
    replaced_urls = set([
    url
    for sublist in df["replacements"]
    for url in sublist])

    # 2. Load already processed URLs
    processed_urls = load_existing_urls_npy(URLS_PATH)

    # 3. Determine which replacement URLs still need processing
    to_process = replaced_urls - processed_urls

    print("to be processed", len(to_process))
    print("already done", len(processed_urls))

    new_embeddings, new_urls = [], []

    # 4. Process only unprocessed replacement URLs
    for img_url in to_process:

        # Download and optionally segment
        image = download_image(img_url)
        if image is None:
            continue
        if segment:
            image = segment_clothing_white(image)

        # Compute embedding
        if extractor =="fashion_clip":
            embedding = encode_image(image)
            # embedding = embedding / torch.linalg.norm(torch.tensor(embedding), ord=2, dim=-1, keepdim=True)
            # embedding = embedding.numpy().astype(np.float32).flatten()
        if extractor == "vit":
            embedding = encode_image_vit(image)

        new_embeddings.append(embedding)
        new_urls.append(img_url)
        processed_urls.add(img_url)  # keep tracking in memory

        # Flush batch
        if len(new_embeddings) >= batch_size:
            flush_embeddings(new_embeddings, new_urls)
            new_embeddings, new_urls = [], []

    # Flush leftovers
    if new_embeddings:
        flush_embeddings(new_embeddings, new_urls)

    print("✅ Finished processing replacement URLs.")

def process_images_parquet(parquet_path, segment=False, batch_size=BATCH_SIZE):
    """
    Process images from a Parquet file and save embeddings/URLs incrementally.
    
    Args:
        parquet_path: path to the input Parquet file
        segment: whether to segment images
        batch_size: number of images before flushing to disk
        embedding_dim: dimension of FashionCLIP embeddings
    """
    df = pd.read_parquet("https://huggingface.co/datasets/traopia/FashionDB/resolve/main/data_vogue_final.parquet")
    df = df[df["category"]=="ready-to-wear"]
    df["image_urls_sample"] = df.apply(
    lambda row: row["image_urls_sample"] + [row["cover_image_url"]],
    axis=1)

    processed_urls = load_existing_urls_npy(URLS_PATH)
    for idx, row in df.iterrows():
        image_urls = row["image_urls_sample"]
        if isinstance(image_urls, str):
            image_urls = [image_urls]

        for img_url in image_urls:
            if img_url in processed_urls:
                #print(f"✅ Skipping {img_url} (already processed)")
                continue

            # Download and optionally segment
            image = download_image(img_url)
            if image is None:
                continue
            if segment:
                image = segment_clothing_white(image)

            # Compute embedding
            embedding = encode_image(image)
            embedding = embedding / torch.linalg.norm(torch.tensor(embedding), ord=2, dim=-1, keepdim=True)
            embedding = embedding.numpy().astype(np.float32).flatten()

            new_embeddings.append(embedding)
            new_urls.append(img_url)
            processed_urls.add(img_url)

            # Flush batch
            if len(new_embeddings) >= batch_size:
                flush_embeddings(new_embeddings, new_urls)
                new_embeddings, new_urls = [], []

    # Flush remaining
    if new_embeddings:
        flush_embeddings(new_embeddings, new_urls)

    print("✅ Finished processing Parquet file.")




def flush_embeddings(batch_embeddings, batch_urls):
    """Append a batch of embeddings and URLs to existing .npy files."""
    # Handle embeddings
    if os.path.exists(EMBEDDINGS_PATH):
        existing_embeddings = np.load(EMBEDDINGS_PATH, allow_pickle=False)
        embeddings_array = np.vstack([existing_embeddings, np.stack(batch_embeddings)])
    else:
        embeddings_array = np.stack(batch_embeddings)
    np.save(EMBEDDINGS_PATH, embeddings_array)

    # Handle URLs
    if os.path.exists(URLS_PATH):
        existing_urls = np.load(URLS_PATH, allow_pickle=True)
        urls_array = np.concatenate([existing_urls, np.array(batch_urls, dtype=object)])
    else:
        urls_array = np.array(batch_urls, dtype=object)
    np.save(URLS_PATH, urls_array)

    print(f"💾 Flushed {len(batch_embeddings)} embeddings. Total now: {embeddings_array.shape[0]}")

def main(extractor):
    #path = "data/data_vogue_final_reviews.parquet"
    process_image_parquet_replaced("data/data_vogue_final_with_images_sampled.parquet", extractor=extractor)





def load_extractor(extractor):
    if extractor == "fashion_clip":
        config = {
            "embeddings_path": "data/embeddings/fashion_clip.npy",
            "urls_path": "data/embeddings/image_urls.npy",
            "model_name": "patrickjohncyh/fashion-clip",
        }

        model = CLIPModel.from_pretrained(config["model_name"]).to(device)
        processor = CLIPProcessor.from_pretrained(config["model_name"])

        return model, processor, config

    elif extractor == "vit":
        config = {
            "embeddings_path": "data/embeddings/vit_embeddings_segmented.npy",
            "urls_path": "data/embeddings/vit_image_urls_segmented.npy",
            "model_name": "vit_base_patch16_224",
        }

        model = timm.create_model(
            config["model_name"],
            pretrained=True,
            num_classes=0,
            global_pool="avg"
        ).to(device)
        model.eval()

        return model, None, config

    else:
        raise ValueError(f"Unknown extractor: {extractor!r}")


if __name__ == "__main__":
    extractor = "fashion_clip"  # or "vit"
    model, processor, cfg = load_extractor(extractor)

    # Set global paths cleanly
    EMBEDDINGS_PATH = cfg["embeddings_path"]
    URLS_PATH = cfg["urls_path"]

    print(f"🚀 Using extractor: {extractor}")
    print(f"📁 Embeddings → {EMBEDDINGS_PATH}")
    print(f"📁 URLs → {URLS_PATH}")

    main(extractor)

    extractor = "vit"  # or "vit"
    model, processor, cfg = load_extractor(extractor)

    # Set global paths cleanly
    EMBEDDINGS_PATH = cfg["embeddings_path"]
    URLS_PATH = cfg["urls_path"]

    print(f"🚀 Using extractor: {extractor}")
    print(f"📁 Embeddings → {EMBEDDINGS_PATH}")
    print(f"📁 URLs → {URLS_PATH}")

    main(extractor)


