import pandas as pd
from extract_info.assign_designer_to_collection import extract_birth_year, extract_names_from_KG
import ast
import numpy as np


df = pd.read_parquet("data/vogue_data_cd.parquet")
df = df[df.groupby("fashion_house")["fashion_house"].transform("count") >= 20]
df["cover_image_url"] = df["image_urls"].apply(
    lambda x: x[0] if isinstance(x, (list, np.ndarray)) and len(x) > 0 else None
)
df = df[["fashion_house", "show", "URL", "cover_image_url", "year", "category","season","location","description","editor","publish_date","designer_name","image_urls","image_urls_sample"]]

df = df.explode("designer_name")
# Remove None or empty strings if any
df = df[df["designer_name"].notna() & (df["designer_name"] != "")]

# bio_brands = pd.read_json("data/scraped_data/brand_data_fmd.json", lines=True)
# bio_brands["foundation_year"] = bio_brands["about"].apply(extract_birth_year)
# df_selected = bio_brands[bio_brands["brand_name"].isin(df.fashion_house.unique())]
# df_extracted_fh = pd.read_json("data/extracted_KG/extracted_KG_fmd_fashion_houses.json", lines=True)
# df_selected  = pd.merge(df_selected, df_extracted_fh[["brand_name","founder"]])
# mask_designer = df_selected["founder"].apply(lambda x: len(x) == 0)
# df_selected.loc[mask_designer, "founder"] = (df_selected.loc[mask_designer, "founded_by"].apply(lambda x: x.copy() if isinstance(x, list) else x))
# df_selected["founder"] = df_selected["founder"].apply(lambda x: x[0] if len(x)>0 else None)
# df_selected["belongs_to"] = df_selected["belongs_to"].apply(lambda x: x[0] if  len(x)>0 else None )

# df = pd.merge(df, df_selected[["brand_name","country","city","website","foundation_year","founder","belongs_to"]], left_on="fashion_house", right_on="brand_name")

df = df[df.groupby("fashion_house")["fashion_house"].transform("count") >= 20]
#df = df.drop_duplicates(subset=["URL"])
df.to_parquet("data/data_vogue_final.parquet")

