import pandas as pd
from extract_info.assign_designer_to_collection import extract_birth_year, extract_names_from_KG
import ast
import numpy as np

df = pd.read_parquet("data/vogue_data_cd.parquet")
df = df[df.groupby("fashion_house")["fashion_house"].transform("count") >= 20]

bio_brands = pd.read_json("data/scraped_data/brand_data_fmd.json", lines=True)
bio_brands["foundation_year"] = bio_brands["about"].apply(extract_birth_year)
df_selected = bio_brands[bio_brands["brand_name"].isin(df.fashion_house.unique())]
df_extracted_fh = pd.read_json("data/extracted_KG/extracted_KG_fmd_fashion_houses.json", lines=True)
df_selected  = pd.merge(df_selected, df_extracted_fh[["brand_name","founder"]])
mask_designer = df_selected["founder"].apply(lambda x: len(x) == 0)
df_selected.loc[mask_designer, "founder"] = (df_selected.loc[mask_designer, "founded_by"].apply(lambda x: x.copy() if isinstance(x, list) else x))
df_selected["founder"] = df_selected["founder"].apply(lambda x: x[0] if len(x)>0 else None)
df_selected["belongs_to"] = df_selected["belongs_to"].apply(lambda x: x[0] if  len(x)>0 else None )
df_selected = df_selected.rename(columns = {"brand_name":"fashion_house"})

#df_orbis = pd.read_excel("data/orbis/fashion_houses_revenues_info_orbis.xlsx", sheet_name="Results")
df_orbis = pd.read_excel("data/orbis/fashion_houses_revenues_info_orbis_matched_city.xlsx", sheet_name="Results")
df_orbis = df_orbis.rename(columns={"Company name Latin alphabet":"brand_name", "NACE Rev. 2, core code (4 digits)": "NACE"})
df_orbis["brand_name"] = df_orbis["brand_name"].ffill()
# Build a mapping: main → list of subs
sub_map = (
    df_orbis.dropna(subset=["SUB - Name"])
      .groupby("brand_name")["SUB - Name"]
      .apply(list)
      .to_dict()
)
df_orbis["subsidiaries"] = df_orbis["brand_name"].map(sub_map)
df_orbis = df_orbis.drop_duplicates(subset=["brand_name"])
df_orbis = df_orbis.drop(columns=[col for col in ["SUB - Name","SUB - Operating revenue (Turnover)\nm USD","Unnamed: 0"] if col in df_orbis.columns])
import re

df_orbis.columns = [
    re.sub(r"Operating revenue \(Turnover\)\s*EUR\s*(\d{4})", r"revenue_\1", col)
    for col in df_orbis.columns]


#df_orbis = df_orbis[df_orbis.groupby("NACE")["NACE"].transform("count") > 1]
#matches = pd.read_excel("data/orbis/fashion_house_matching.xlsx")
matches = pd.read_excel("data/orbis/fashion_house_matching_city.xlsx")
matches  = matches .rename(columns={"Company name":"fashion_house"})
df_orbis = pd.merge(df_orbis, matches[["fashion_house", "Matched company name"]], left_on="brand_name", right_on="Matched company name")

df_selected = pd.merge(df_selected, df_orbis, how="outer")

df_selected = df_selected.replace("n.a.", None)

df_selected = df_selected.apply(lambda col: pd.to_numeric(col, errors="ignore"))
df_selected.to_parquet("data/final_info_fh.parquet")