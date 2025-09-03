import ast
from rapidfuzz import process, fuzz
import pandas as pd
from extract_info.assign_designer_to_collection import extract_birth_year
from extract_info.sparql_query_wikidata import get_wikidata_info_based_on_id
import re
import numpy as np

def extract_names_from_KG(kg_string, properties):
    all_names = []
    try:
        # Convert string to Python dict safely
        kg_dict = ast.literal_eval(kg_string)
        for property in properties:
            # Extract from specified properties
            names = [entry[0] for entry in kg_dict.get(property, []) if entry]
            all_names += names  # <- fix here
        
        return all_names
    except Exception:
        return []  # If parsing fails



def match_school(name, choices, score_cutoff=80):
    if not name:
        return None
    match, score, _ = process.extractOne(name, choices, scorer=fuzz.token_sort_ratio)
    if score >= score_cutoff:
        return match
    else:
        return None



import spacy

# Load English NER model (you can use a larger one like en_core_web_trf if needed)
nlp = spacy.load("en_core_web_sm")

def extract_first_place(text):
    """
    Extract the first place or country mentioned in a biography text.
    Returns None if no place is found.
    """
    if not isinstance(text, str) or not text.strip():
        return None
    
    doc = nlp(text)
    for ent in doc.ents:
        if ent.label_ in ("GPE", "LOC"):  # GPE = countries, cities, states; LOC = other locations
            return ent.text
    return None

def assign_place_of_birth(df, biography_col="biography"):
    """
    Add a 'place_of_birth' column to df by extracting the first mentioned location in the biography column.
    """
    df = df.copy()
    df["place_of_birth"] = df[biography_col].apply(extract_first_place)
    return df


bio_designers = pd.read_json("data/scraped_data/designer_data_fmd.json", lines=True)
bio_designers["year_birth"] = bio_designers["biography"].apply(extract_birth_year)
bio_designers = assign_place_of_birth(bio_designers, biography_col="biography")

extracted_designers = pd.read_json("data/extracted_KG/extracted_KG_fmd_fashion_designers.json", lines=True)
extracted_designers['school'] = extracted_designers['KG'].apply(extract_names_from_KG, properties=["educated_at"])
extracted_designers['school'] = extracted_designers['school'].apply(lambda x: x[0] if len(x)>0 else [])
extracted_designers['employer'] = extracted_designers['KG'].apply(extract_names_from_KG, properties=["employer"])
school_list = pd.read_csv("data/names/school_names_designers_wikidata.csv").schoolLabel
# Apply fuzzy matching
extracted_designers["education"] = extracted_designers["school"].apply(lambda x: match_school(x, school_list))
bio_designers = pd.merge(bio_designers, extracted_designers[["designer_name","education","employer"]])

bio_designers = bio_designers[["designer_name","year_birth", "place_of_birth","education","employer"]]



df = pd.read_parquet("data/data_vogue_final.parquet")
df = df[df.groupby("fashion_house")["fashion_house"].transform("count") >= 20]

wiki_names_df = pd.read_csv("data/names/fashion_designers_wikidata.csv")
wiki_names = wiki_names_df.designer_name.tolist()
names_needed_fromwiki = [name for name in df.designer_name.unique() if name in wiki_names]
names_needed_fromwiki_id = wiki_names_df[wiki_names_df["designer_name"].isin(names_needed_fromwiki)].fashionDesigner

df_info = get_wikidata_info_based_on_id(names_needed_fromwiki_id)

df_info["educationLabel"] = df_info["educationLabel"].apply(lambda x: x[0] if len(x)> 0 else None)
df_info["education"] = df_info["educationLabel"].apply(lambda x: match_school(x, school_list))
df_info["year_birth"] = pd.to_datetime(df_info["dateOfBirth"]).dt.year.astype(float)
#df_info = df_info.replace({np.nan: None})
df_info = df_info.rename(columns={"personLabel":"designer_name","placeOfBirthLabel":"place_of_birth", "countryLabel":"nationality"})
df_info = df_info[["designer_name","place_of_birth","year_birth","education", "nationality"]]

designer_info_df = pd.merge(df_info,bio_designers,  how="outer")

designer_info_df.to_parquet("data/final_info_designers.parquet")