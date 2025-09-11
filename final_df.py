import pandas as pd
from extract_info.assign_designer_to_collection import extract_birth_year, extract_names_from_KG
import ast
import numpy as np


#df = pd.read_parquet("data/vogue_data_cd.parquet")
df = pd.read_parquet("data/data_vogue_final_reviews.parquet")
df = df[df.groupby("fashion_house")["fashion_house"].transform("count") >= 20]
df["cover_image_url"] = df["image_urls"].apply(
    lambda x: x[0] if isinstance(x, (list, np.ndarray)) and len(x) > 0 else None
)
if "annotation" not in df.columns:
    print("annotation of descriptions not found")

df = df[["fashion_house", "show", "URL", "cover_image_url", "year", "category","season","location","description","editor","publish_date","designer_name","image_urls","image_urls_sample", "annotation"]]

df = df.explode("designer_name")
# Remove None or empty strings if any
df = df[df["designer_name"].notna() & (df["designer_name"] != "")]


if "image_urls_sample" in df.columns:
    def _ensure_list(x):
        if isinstance(x, np.ndarray):
            try:
                return [u for u in x.tolist() if pd.notna(u)]
            except Exception:
                return []
        if isinstance(x, (list, tuple, set)):
            return [u for u in list(x) if pd.notna(u)]
        if pd.isna(x):
            return []
        return [x]
    df["image_urls_sample"] = df["image_urls_sample"].apply(_ensure_list)
else:
    df["image_urls_sample"] = [[] for _ in range(len(df))]
if "cover_image_url" in df.columns:
    def _add_cover(row):
        urls = row["image_urls_sample"]
        cover = row["cover_image_url"]
        if isinstance(cover, str) and cover.startswith(("http://", "https://")) and cover not in urls:
            return [cover] + urls
        return urls
    df["image_urls_sample"] = df.apply(_add_cover, axis=1)



df.to_parquet("data/data_vogue_final_reviews.parquet")

