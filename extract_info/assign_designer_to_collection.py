import pandas as pd
import re 
import ahocorasick  
import unicodedata
from thefuzz import fuzz, process
import numpy as np
from collections import Counter
import json


from collections import defaultdict

def fashion_house_designer_periods(df):
    # First explode designer_name so each designer has its own row
    df_exp = df.explode("designer_name")
    
    # Remove None or empty string designers if any
    df_exp = df_exp[df_exp["designer_name"].notna() & (df_exp["designer_name"] != "")]
    
    # Group by fashion_house and designer_name, aggregate min and max year
    grouped = df_exp.groupby(["fashion_house", "designer_name"])["year"].agg(["min", "max"]).reset_index()
    
    # Build dictionary with desired structure
    result = defaultdict(list)
    for _, row in grouped.iterrows():
        result[row["fashion_house"]].append({
            "designer": row["designer_name"],
            "start_year": int(row["min"]),
            "end_year": int(row["max"])
        })
    return dict(result)


def to_list(x):
    if isinstance(x, list):
        return x
    if isinstance(x, np.ndarray):
        return x.tolist()
    if x is None:
        return []
    return [x] if not isinstance(x, (list, tuple)) else list(x)

def assign_designer_to_fashion_house(df, fashion_house, designer_name):

    mask = df["fashion_house"] == fashion_house
    if mask.any():
        df.loc[mask, "designer_name"] = pd.Series([designer_name] * mask.sum(), index=df.index[mask])
    else:
        print(f"No rows found for fashion house: {fashion_house}")
    return df


def extract_birth_year(text, min_year=1850, max_year=2025):
    if not isinstance(text, str):
        return None
    years = re.findall(r"\b(1[89]\d{2}|20\d{2})\b", text)  # captures 1800–2099
    years = [int(y) for y in years if min_year <= int(y) <= max_year]
    #return min(years) if years else None
    return years[0] if years else None

def propagate_single(row, df, founder_lookup):
    fh, yr = row.fashion_house, row.year
    current_names = row.designer_names

    # Helper to compare designer lists ignoring order
    def same_names(list1, list2):
        return set(list1) == set(list2)

    # If multiple designers but more than 2, treat as ambiguous and propagate
    if len(current_names) > 2:
        current_names = []

    # If multiple designers with 2 or fewer names,
    # check if exact same list appears in neighbor years
    elif 1 < len(current_names) <= 2:
        # Prioritize founder if present in current_names
        founders = founder_lookup.get(fh, [])
        if founders:
            # intersect founders and current_names
            founders_in_current = [name for name in current_names if name in founders]
            if founders_in_current:
                # prioritize founders only
                current_names = founders_in_current
            # else keep current_names as is (no founders present)

        years_to_check = range(yr - 3, yr + 3)
        neighbors = df.loc[
            (df.fashion_house == fh) &
            (df.year.isin(years_to_check)) &
            (df.index != row.name)
        ]
        # Check if any neighbor has exactly the same designer list (order ignored)
        repeated = all(
            any(same_names(x, current_names) for x in df[df.year == y]["designer_names"])
            for y in years_to_check if y != yr
        )
        if not repeated:
            current_names = []

    # If single designer but appears only once in the whole fashion house, treat as empty
    if len(current_names) == 1:
        name = current_names[0]
        count_in_house = sum(
            len(names) == 1 and names[0] == name
            for names in df.loc[df.fashion_house == fh, "designer_names"]
        )
        if count_in_house == 1:
            current_names = []

    # If empty now, try to propagate from neighbors with exactly one designer
    if len(current_names) == 0:
        neighbors = df.loc[
            (df.fashion_house == fh) &
            (df.year.isin([yr - 2, yr, yr + 2])) &
            (df.index != row.name)
        ]
        single_neighbors = neighbors[neighbors["designer_names"].apply(lambda x: len(x) == 1)]

        if not single_neighbors.empty:
            single_neighbors = single_neighbors.iloc[
                (single_neighbors["year"] - yr).abs().argsort()
            ]
            return single_neighbors["designer_names"].iloc[0]

    return current_names



def fill_empty_designer_names(df):
    # Ensure every value in designer_name is a list
    df = df.copy()
    df["designer_name"] = df["designer_name"].apply(to_list)

    # Ensure sorted by fashion_house and year
    df = df.sort_values(["fashion_house", "year"]).reset_index(drop=True)

    # Copy to modify
    filled = df["designer_name"].copy()

    # Fill within each fashion house
    for fh, group in df.groupby("fashion_house"):
        indices = group.index.values
        years = group["year"]

        for i in indices:
            if len(df.at[i, "designer_name"]) == 0:
                year = df.at[i, "year"]

                # Find nearest year with a non-empty list
                non_empty = [
                    (idx, abs(year - df.at[idx, "year"]))
                    for idx in indices
                    if len(df.at[idx, "designer_name"]) > 0
                ]

                if non_empty:
                    closest_idx = min(non_empty, key=lambda x: x[1])[0]
                    filled.at[i] = df.at[closest_idx, "designer_name"]

    df["designer_name"] = filled
    return df



import ast

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
    

def any_name_in_designers(names, designer_list):
    return any(name in designer_list for name in names)



from collections import Counter
from fuzzywuzzy import fuzz

def clean_and_merge_names(name_set, threshold=90, min_count=1):
    # Step 1: Remove None, empty, whitespace-only
    cleaned = [n.strip() for n in name_set if n and n.strip()]
    
    # Step 2: Convert to title case
    cleaned = [n.title() for n in cleaned]
    
    # Step 3: Keep only names with at least two words
    cleaned = [n for n in cleaned if len(n.split()) >= 2]
    
    # Step 4: Keep only names with at least `min_count` occurrences
    counts = Counter(cleaned)
    cleaned = {n for n in cleaned if counts[n] >= min_count}
    
    # Step 5: Merge similar names
    merged = set()
    processed = set()
    
    for name in cleaned:
        if name in processed:
            continue
        
        # Find all similar names
        similar = [n for n in cleaned if fuzz.ratio(name, n) >= threshold]
        
        # Choose canonical as shortest for now
        canonical = sorted(similar, key=lambda x: len(x))[0]
        merged.add(canonical)
        
        # Mark similar ones as processed
        processed.update(similar)
    
    return merged


def is_close_match(brand_name, choices, threshold=90):
    # Returns True if best match score >= threshold
    best_match, score = process.extractOne(brand_name, choices)
    return score >= threshold



def strip_accents(text):
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )


def build_founder_lookup(df_fh_fmd, df_extracted_fashion_house, fashion_houses):
    # From FMD brand data
    founder_lookup_all = (
        df_fh_fmd[df_fh_fmd["founded_by"].apply(lambda x: x != [None])]
        .set_index("brand_name")["founded_by"]
        .to_dict()
    )

    # From extracted KG
    founder_lookup_extr = df_extracted_fashion_house.set_index("brand_name")["founder"].to_dict()

    # Merge — only add if not already present
    founder_lookup = {k: founder_lookup_all[k] for k in fashion_houses if k in founder_lookup_all}
    for k, v in founder_lookup_extr.items():
        if k not in founder_lookup:
            founder_lookup[k] = v

    return founder_lookup


def build_automaton(names):
    A = ahocorasick.Automaton()
    for name in names:
        normalized = strip_accents(name.lower())
        A.add_word(normalized, name)
    A.make_automaton()
    return A


def find_names(text, automaton, threshold=85):
    if not isinstance(text, str):
        return []

    normalized_text = strip_accents(text.lower())
    candidates = set()

    for end_idx, orig_name in automaton.iter(normalized_text):
        start_idx = end_idx - len(strip_accents(orig_name.lower())) + 1
        matched_substring = text[start_idx:end_idx+1]
        candidates.add((orig_name, matched_substring))

    return list({
        orig_name
        for orig_name, matched in candidates
        if fuzz.ratio(strip_accents(orig_name.lower()), strip_accents(matched.lower())) >= threshold
    })


from collections import Counter
import numpy as np

def replace_one_off_designers(df):
    # Ensure designer_name is always a list
    df = df.copy()
    df["designer_name"] = df["designer_name"].apply(to_list)

    # Sort for closest-year logic
    df = df.sort_values(["fashion_house", "year"]).reset_index(drop=True)
    filled = df["designer_name"].copy()

    for fh, group in df.groupby("fashion_house"):
        indices = group.index.tolist()

        # Flatten all names for this fashion house
        all_names = [
            name for sublist in group["designer_name"]
            for name in sublist
            if name and str(name).strip() != ""
        ]
        counts = Counter(all_names)

        # Designers that appear <= 4 times
        one_offs = {name for name, cnt in counts.items() if cnt <= 4}

        for idx in indices:
            current_names = df.at[idx, "designer_name"]

            for rare_name in list(current_names):
                if rare_name in one_offs:
                    year = df.at[idx, "year"]

                    # Find closest row with non-empty designer_name not containing rare_name
                    candidates = [
                        (other_idx, abs(year - df.at[other_idx, "year"]))
                        for other_idx in indices
                        if other_idx != idx
                        and len(df.at[other_idx, "designer_name"]) > 0
                        and rare_name not in df.at[other_idx, "designer_name"]
                    ]
                    if candidates:
                        closest_idx = min(candidates, key=lambda x: x[1])[0]
                        replacement_names = df.at[closest_idx, "designer_name"]

                        # Merge old and new names, removing the rare one
                        merged = [n for n in current_names if n != rare_name] + list(replacement_names)
                        merged = deduplicate_and_split(merged)
                        filled.at[idx] = merged

    df["designer_name"] = filled
    return df

def split_names(full_name):
    """Split strings like 'A & B', 'A and B', or 'A, B' into individual names."""
    parts = re.split(r"\s*&\s*|\s+and\s+|,", full_name)
    return [p.strip() for p in parts if p.strip()]

def deduplicate_and_split(names):
    """
    - Split compound names like "Viktor & Rolf" into ["Viktor", "Rolf"]
    - Remove shorter semi-duplicates when a longer name exists
    """
    if not isinstance(names, np.ndarray) or not isinstance(names, list):
        return names

    # Step 1: split all compound names
    all_names = []
    for n in names:
        if not n or not isinstance(n, str):
            continue
        all_names.extend(split_names(n))

    # Step 2: remove semi-duplicates (keep longest/more complete)
    final_names = set(all_names)
    to_remove = set()
    for name in final_names:
        for other in final_names:
            if name == other:
                continue
            # Remove shorter name if fully contained in another with extra words
            if name in other and len(other.split()) > len(name.split()):
                to_remove.add(name)

    final_names -= to_remove
    return sorted(final_names)

import spacy
import pandas as pd
import os


def extract_person_names(text):
    if not isinstance(text, str):
        return []
    doc = nlp(text)
    persons = [ent.text for ent in doc.ents if ent.label_ == "PERSON"]
    return list(set(persons))  # unique names only




if __name__ == "__main__":
    # === Load and filter main DF ===
    df = pd.read_parquet("data/vogue_data.parquet")
    df = df[df.groupby("fashion_house")["fashion_house"].transform("count") >= 10]
    df = df.drop(columns=[c for c in ["designer_names", "designer_name", "designer_source"] if c in df.columns])

    designers = pd.read_json("data/names/fashion_show_data_all_designer.json", lines=True)
    df = pd.merge(df, designers[["URL", "designer_final"]])
    # === Load designer sources ===
    bio_designers = pd.read_json("data/scraped_data/designer_data_fmd.json", lines=True)
    bio_designers["year_birth"] = bio_designers["biography"].apply(extract_birth_year)
    designers_fmd = bio_designers[bio_designers["year_birth"] > 1910].designer_name

    designer_bof = pd.read_json("data/scraped_data/all_designer_data_BOF.json", lines=True).designer_name
    additional_designers = pd.read_csv("data/names/additional_designers.csv").designer_name

    df = df.dropna(subset=["description"])  # just in case

    # Apply NER extraction to 'description'
    if os.path.exists("data/names/all_ner_names.csv"):
        all_ner_names = pd.read_csv("data/names/all_ner_names.csv").designer_name
    else:
        nlp = spacy.load("en_core_web_sm")
        df["ner_person_names"] = df["description"].apply(extract_person_names)
        all_ner_names = list(name for sublist in df["ner_person_names"] for name in sublist)
        all_ner_names = clean_and_merge_names(all_ner_names, threshold=90, min_count=20)
        all_ner_names_df = pd.DataFrame(all_ner_names, columns=["designer_name"])
        all_ner_names_df.to_csv("data/names/all_ner_names.csv", index = False)

    # === Founders from extracted KG ===
    df_extracted_fashion_house = pd.read_json("data/extracted_KG/extracted_KG_fmd_fashion_houses.json", lines=True)
    unique_houses = df['fashion_house'].dropna().unique()
    mask = df_extracted_fashion_house['brand_name'].apply(lambda x: is_close_match(x, unique_houses))
    df_extracted_filtered = df_extracted_fashion_house[mask]

    df_extracted_filtered['names_in_KG'] = df_extracted_filtered['KG'].apply(extract_names_from_KG, properties=["founded_by", "designer_employed"])
    df_extracted_filtered['founder'] = df_extracted_filtered['KG'].apply(extract_names_from_KG, properties=["founded_by"])

    founders = {name for sublist in df_extracted_filtered["founder"] for name in sublist}
    founders = clean_and_merge_names(founders, threshold=90)

    # === All designer names ===
    all_designers = set(designers_fmd) | set(designer_bof) | set(additional_designers) | set(founders) #| set(all_ner_names)


    # === Build founder lookup dict ===
    df_fh_fmd = pd.read_json("data/scraped_data/brand_data_fmd.json", lines=True)
    founder_lookup = build_founder_lookup(df_fh_fmd, df_extracted_fashion_house, df.fashion_house.unique())

    # === Build and apply automaton ===
    automaton = build_automaton(all_designers)
    df = df.dropna(subset=["description"])
    df["designer_names"] = df["description"].apply(lambda t: find_names(t, automaton))

    #if designer_names empty automata with NER names 

    mask_designer = df["designer_names"].apply(lambda x: len(x) == 0)
    df.loc[mask_designer, "designer_names"] = (df.loc[mask_designer, "designer_final"].apply(lambda x: x.copy() if isinstance(x, list) else x))


    # Assign founders for small fashion houses (<30 rows)
    small_fhs = df['fashion_house'].value_counts()
    small_fhs = small_fhs[small_fhs < 25].index.tolist()
    mask_small_fh = df["fashion_house"].isin(small_fhs)

    df.loc[mask_small_fh, "designer_name"] = df.loc[mask_small_fh, "fashion_house"].map(founder_lookup)



    # === Fill missing designer_name from founders ===
    df["year"] = df["year"].astype(int)
    df = df.sort_values(["fashion_house", "year"]).reset_index(drop=True)
    df["designer_name"] = df["designer_name"].apply(to_list)
    df["designer_name"] = df.apply(lambda r: propagate_single(r, df, founder_lookup), axis=1)

    mask_designer = df["designer_names"].apply(lambda x: len(x) == 0)
    automaton = build_automaton(all_ner_names)
    df.loc[mask_designer, "designer_names"] = (
        df.loc[mask_designer, "description"]  # or the column with text to search
        .apply(lambda text: find_names(text, automaton)))

    mask_designer = df["designer_name"].apply(lambda x: len(x) == 0)
    mask_fh = df['fashion_house'].isin(founder_lookup.keys())
    df.loc[mask_designer, "designer_name"] = df.loc[mask_fh, "fashion_house"].map(founder_lookup)

    # === Final fill pass ===
    #df = fill_empty_designer_names(df)

    #remove one off designers as probably spurious 
    
    df = replace_one_off_designers(df)
    df["designer_name"] = df["designer_name"].apply(to_list)
    df["designer_name"] = df["designer_name"].apply(lambda x: [] if x == [None] else x)

    df = assign_designer_to_fashion_house(df, "Aquascutum", ["John Emary"])
    df = assign_designer_to_fashion_house(df, "Area", ["Beckett Fogg", "Piotrek Panszczyk"])
    df = assign_designer_to_fashion_house(df, "Nehera", ["Samuel Drira"])
    df = assign_designer_to_fashion_house(df, "Matthew Williamson", ["Matthew Williamson"])
    df = assign_designer_to_fashion_house(df, "Limi Feu" ,[ "Limi Yamamoto"])
    df = assign_designer_to_fashion_house(df,"Lazoschmidl" ,["Johannes Schmidl"] )
    df = assign_designer_to_fashion_house(df,"Kolor" ,["Junichi Abe"] )
    df = assign_designer_to_fashion_house(df,"Kiton" , ["Ciro Paone"] )
    df = assign_designer_to_fashion_house(df, "Audra", ["Audra Noyes" ])
    df = assign_designer_to_fashion_house(df, "Au Jour Le Jour", [ "Diego Marquez", "Mirko Fontana"])
    df = assign_designer_to_fashion_house(df, "Babyghost", ["Qiaoran Huang","Joshua Hupper"] )
    df = assign_designer_to_fashion_house(df,"Badgley Mischka" , ["James Mischka", "Mark Badgley"] )
    df = assign_designer_to_fashion_house(df,"Baja East" , ["John Targon", "Scott Studenberg"] )
    df = assign_designer_to_fashion_house(df, "Bally", ["Rhuigi Villaseñor"] )
    df = assign_designer_to_fashion_house(df, "Blumarine", ["Anna Molinari"] )
    df = assign_designer_to_fashion_house(df, "Boglioli", ["Davide Marello"] )
    df = assign_designer_to_fashion_house(df, "Brian Reyes", ["Brian Reyes"] )
    df = assign_designer_to_fashion_house(df, "Zankov", ["Henry Zankov"] )
    df = assign_designer_to_fashion_house(df, "Lindsey Thornburg", ["Lindsey Thornburg"] )
    df = assign_designer_to_fashion_house(df, "Aganovich", ["Nana Aganovich"] )
    df = assign_designer_to_fashion_house(df, "William Fan", ["William Fan"] )
    df = assign_designer_to_fashion_house(df, "Courreges", ["Andre Courreges"] )
    df = assign_designer_to_fashion_house(df, "Maurizio Pecoraro", ["Maurizio Pecoraro"] )
    df = assign_designer_to_fashion_house(df, "Corneliani", ["Sergio Corneliani"] )
    df = assign_designer_to_fashion_house(df, "Frederick Anderson", ["Frederick Anderson"] )
    df = assign_designer_to_fashion_house(df, "Issa", ["Chetana Sagiraju","Swati Yendluri"] )
    df = assign_designer_to_fashion_house(df, "Josie Natori", ["Josie Natori"] )
    df = assign_designer_to_fashion_house(df, "Katie Gallagher", ["Katie Gallagher"] )
    df = assign_designer_to_fashion_house(df, "Talbot Runhof", ["Johnny Talbot","Adrian Runhof"] )
    df = assign_designer_to_fashion_house(df, "Wendy Nichol", ["Wendy Nichol"] )
    df = assign_designer_to_fashion_house(df, "Nomia", ["Yara Flinn"] )
    df = assign_designer_to_fashion_house(df, "Vetements", ["Demna Gvasalia"] )
    df = assign_designer_to_fashion_house(df, "Viktor & Rolf", ["Viktor Horsting","Rolf Snoereg"] )
    df = assign_designer_to_fashion_house(df, "Viktor Rolf", ["Viktor Horsting","Rolf Snoereg"] )
    df = assign_designer_to_fashion_house(df, "Nicholas K", ["Nicholas K"] )
    df = assign_designer_to_fashion_house(df, "Bach Mai", ["Bach Mai"] )
    # Save


    df["designer_name"] = df["designer_name"].apply(deduplicate_and_split)
    df= df[df["designer_name"].apply(lambda x: len(x) !=0)]
    #df = df.drop(columns=[col for col in ["designer_names","ner_person_names"] if col in df.columns])
    df.to_parquet("data/vogue_data_cd.parquet")

    df_exp = df.explode("designer_name")

    # Remove None or empty strings if any
    df_exp = df_exp[df_exp["designer_name"].notna() & (df_exp["designer_name"] != "")]

    # Count distinct designers per fashion house
    designer_counts = df_exp.groupby("fashion_house")["designer_name"].nunique()

    # Filter fashion houses with more than one distinct designer
    fh_multiple_designers = designer_counts[designer_counts > 1].index.tolist()

    # Optionally get df filtered by these fashion houses
    df_multiple_designers = df[df["fashion_house"].isin(fh_multiple_designers)]
    periods_dict = fashion_house_designer_periods(df_multiple_designers)
    with open('data/creative_directors_timelines.json', 'w') as fp:
        json.dump(periods_dict, fp, indent=2, sort_keys=True)


