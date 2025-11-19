import torch
import timm
import torchvision.transforms as T
from PIL import Image
import numpy as np
import pandas as pd
import requests, os
from io import BytesIO
from extract_clip_visual import download_image, segment_clothing_white

# -----------------------------
# Paths and parameters
# -----------------------------
EMBEDDINGS_PATH = "data/embeddings/vit_embeddings_segmented.npy"   # change name if desired
URLS_PATH = "data/embeddings/vit_image_urls_segmented.npy"
BATCH_SIZE = 50

device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
print(f"🧠 Using device: {device}")

# -----------------------------
# Model: choose one
# -----------------------------
# model_name = "resnet50"
model_name = "vit_base_patch16_224"

model = timm.create_model(model_name, pretrained=True, num_classes=0, global_pool="avg").to(device)
model.eval()

# Expected embedding dim:
example = torch.randn(1, 3, 224, 224)
with torch.no_grad():
    out = model(example.to(device))
print(f"✅ Model output dim: {out.shape[1]}")

# -----------------------------
# Preprocessing transform
# -----------------------------
transform = T.Compose([
    T.Resize((224, 224)),
    T.ToTensor(),
    T.Normalize(mean=(0.485, 0.456, 0.406), std=(0.229, 0.224, 0.225))
])

# -----------------------------
# Helper: download and load images (keep your version)
# -----------------------------
# def download_image(image_url):
#     try:
#         response = requests.get(image_url, timeout=5)
#         response.raise_for_status()
#         image = Image.open(BytesIO(response.content)).convert("RGB")
#         return image
#     except Exception as e:
#         print(f"⚠️ Failed to download {image_url}: {e}")
#         return None


# -----------------------------
# Encode function (new)
# -----------------------------
def encode_image(image):
    """Encode an image with timm ResNet or ViT backbone."""
    image_tensor = transform(image).unsqueeze(0).to(device)
    with torch.no_grad():
        features = model(image_tensor)
    embedding = features.cpu().numpy().flatten().astype(np.float32)
    embedding /= np.linalg.norm(embedding) + 1e-8  # normalize
    return embedding


# -----------------------------
# Incremental save logic
# -----------------------------
def flush_embeddings(batch_embeddings, batch_urls):
    if os.path.exists(EMBEDDINGS_PATH):
        existing_embeddings = np.load(EMBEDDINGS_PATH, allow_pickle=False)
        embeddings_array = np.vstack([existing_embeddings, np.stack(batch_embeddings)])
    else:
        embeddings_array = np.stack(batch_embeddings)
    np.save(EMBEDDINGS_PATH, embeddings_array)

    if os.path.exists(URLS_PATH):
        existing_urls = np.load(URLS_PATH, allow_pickle=True)
        urls_array = np.concatenate([existing_urls, np.array(batch_urls, dtype=object)])
    else:
        urls_array = np.array(batch_urls, dtype=object)
    np.save(URLS_PATH, urls_array)

    print(f"💾 Flushed {len(batch_embeddings)} embeddings. Total now: {embeddings_array.shape[0]}")


# -----------------------------
# Main loop (same as yours)
# -----------------------------
def process_images_parquet(parquet_path, batch_size=BATCH_SIZE):
    df = pd.read_parquet(parquet_path)
    df["image_urls_sample"] = df.apply(
    lambda row: row["image_urls_sample"] + [row["cover_image_url"]],
    axis=1)
    df = df[df["category"] == "ready-to-wear"]
    if "image_urls_sample" not in df.columns:
        raise ValueError("Parquet must have 'image_urls_sample' column")

    if os.path.exists(URLS_PATH):
        processed = set(np.load(URLS_PATH, allow_pickle=True))
        print("processed already", len(processed))
    else:
        processed = set()

    new_embeddings, new_urls = [], []

    for _, row in df.iterrows():
        urls = row["image_urls_sample"]
        if isinstance(urls, str):
            urls = [urls]

        for url in urls:
            if url in processed:
                continue

            img = download_image(url)
            if img is None:
                continue
            img = segment_clothing_white(img)
            if img is None:
                continue

            emb = encode_image(img)
            new_embeddings.append(emb)
            new_urls.append(url)
            processed.add(url)

            if len(new_embeddings) >= batch_size:
                flush_embeddings(new_embeddings, new_urls)
                new_embeddings, new_urls = [], []

    if new_embeddings:
        flush_embeddings(new_embeddings, new_urls)

    print("✅ Finished processing.")

from extract_clip_visual import clean_row, load_existing_urls_npy

def process_image_parquet_replaced(parquet_path, segment=False, batch_size=BATCH_SIZE, embedding_dim=512):
    df = pd.read_parquet("https://huggingface.co/datasets/traopia/FashionDB/resolve/main/data_vogue_final.parquet")
    df = df[df["category"]=="ready-to-wear"]
    df["image_urls_sample"] = df.apply(
    lambda row: row["image_urls_sample"] + [row["cover_image_url"]],
    axis=1)
    bad_urls = (
    df["image_urls_sample"]
    .explode()
    .dropna()
    .loc[lambda s: s.str.contains(r"(beauty|detail)", case=False, na=False)])
    print("total to replace",len(bad_urls))

    df["cleaned_image_urls_sample"] = df.apply(clean_row, axis=1)

    substituted_urls = [
    (old, new)
    for old, new in zip(df["image_urls_sample"], df["cleaned_image_urls_sample"])
    if list(old) != list(new)]

    replaced_urls = [
        url
        for old_list, new_list in substituted_urls
        for url in list(new_list)
        if url not in list(old_list)
    ]
    df = df.drop(columns=["image_urls_sample"])
    df = df.rename(columns={"cleaned_image_urls_sample":"image_urls_sample"})
    if "image_urls_sample" not in df.columns:
        raise ValueError("Parquet file must have 'image_urls_sample' column")
    
    processed_urls = load_existing_urls_npy(URLS_PATH)
    remaining = set(df["image_urls_sample"].explode()) - processed_urls
    print("to be processed", len(remaining))
    print("already done", len(processed_urls))
    new_embeddings, new_urls = [], []

    for img_url in replaced_urls:
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


# -----------------------------
# Run
# -----------------------------
def main():
    parquet_path = "data/data_vogue_final.parquet"
    process_image_parquet_replaced(parquet_path,segment=True)

if __name__ == "__main__":
    main()