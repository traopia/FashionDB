
import torch
import requests
import json
from io import BytesIO
from PIL import Image
from transformers import CLIPProcessor, CLIPModel
import os
import pandas as pd
import os
import numpy as np
import torch
import pandas as pd


EMBEDDINGS_PATH = "data/embeddings/fashion_clip.npy"
URLS_PATH = "data/embeddings/image_urls.npy"
BATCH_SIZE = 50  # flush every 50 images

#set device: Use GPU if availanle, otherwise mps if available otherwise CPU
device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"


# Load Fashion-CLIP model and processor
model_name = "patrickjohncyh/fashion-clip"
#model_name = "openai/clip-vit-base-patch32"
model = CLIPModel.from_pretrained(model_name).to(device)
processor = CLIPProcessor.from_pretrained(model_name)


from transformers import pipeline
from PIL import Image
import numpy as np
import os
import torch
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

        image.save(f'/Users/{device_dir}/Library/CloudStorage/OneDrive-UvA/fashion_images/images_all/'+sanitized_filename, format="JPEG")
        print(f"✅ Image saved as {sanitized_filename}")
        
        return image
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Failed to download {image_url}: {e}")
        return None
    
def get_image_locally(image_url):
    """Retrieve an image from local storage based on its sanitized filename."""
    sanitized_filename = image_url.replace("://", "-").replace("/", "_")
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
    else:
        print(f"⚠️ Image not found locally at {local_path}")
        return None


def encode_image(image):
    """Encode image into an embedding."""
    inputs = processor(images=image, return_tensors="pt").to(device)

    with torch.no_grad():
        image_features = model.get_image_features(**inputs).cpu().numpy()  # Move to CPU for stability
    return image_features



def load_existing_urls_npy(urls_path=URLS_PATH):
    """Load already processed image URLs from .npy file."""
    if not os.path.exists(urls_path):
        return set()
    urls_array = np.load(urls_path, allow_pickle=True)
    return set(urls_array)

def process_images_parquet(parquet_path, segment=False, batch_size=BATCH_SIZE, embedding_dim=512):
    """
    Process images from a Parquet file and save embeddings/URLs incrementally.
    
    Args:
        parquet_path: path to the input Parquet file
        segment: whether to segment images
        batch_size: number of images before flushing to disk
        embedding_dim: dimension of FashionCLIP embeddings
    """
    df = pd.read_parquet(parquet_path)
    if "image_urls_sample" not in df.columns:
        raise ValueError("Parquet file must have 'image_urls_sample' column")
    
    processed_urls = load_existing_urls_npy(URLS_PATH)
    new_embeddings, new_urls = [], []

    for idx, row in df.iterrows():
        image_urls = row["image_urls_sample"]
        if isinstance(image_urls, str):
            image_urls = [image_urls]

        for img_url in image_urls:
            if img_url in processed_urls:
                print(f"✅ Skipping {img_url} (already processed)")
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

def main():
    path = "data/vogue_data.parquet"
    process_images_parquet(path, segment=True)



if __name__ == "__main__":
    main()


