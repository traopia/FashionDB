import pandas as pd
import os
from sparql_query_wikibase import *
import spacy
import unicodedata
import json


# Load spaCy NER model
nlp = spacy.load("en_core_web_sm")


designer_label_query = """PREFIX wbt: <https://fashionwiki.wikibase.cloud/prop/direct/>
        PREFIX wb: <https://fashionwiki.wikibase.cloud/entity/>
        SELECT ?designerLabel
        WHERE {
        ?designer wbt:P2 wb:Q5.
        SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
        } """
designer_creative_directors_query = """ PREFIX wbt: <https://fashionwiki.wikibase.cloud/prop/direct/>
    PREFIX wb: <https://fashionwiki.wikibase.cloud/entity/>
    PREFIX pq: <https://fashionwiki.wikibase.cloud/prop/qualifier/>  
    PREFIX pr: <https://fashionwiki.wikibase.cloud/prop/reference/>
    PREFIX ps: <https://fashionwiki.wikibase.cloud/prop/statement/> 
    PREFIX p: <https://fashionwiki.wikibase.cloud/prop/> 
    PREFIX prov: <http://www.w3.org/ns/prov#>  

    SELECT  ?fashion_houseLabel ?designerLabel
        (COALESCE(YEAR(?start_time), YEAR(?point_time), YEAR(?inception)) AS ?start_year) 
        (YEAR(?end_time) AS ?end_year) 
    ?title
        
    WHERE {
    # Designer linked to fashion house through roles or founded_by

    {
    ?fashion_house p:P29 ?statement_designer.
    ?statement_designer ps:P29 ?designer
    
    OPTIONAL { ?statement_designer pq:P15 ?start_time. }
    OPTIONAL { ?statement_designer pq:P28 ?point_time. } 
    OPTIONAL { ?statement_designer pq:P16 ?end_time. }
    OPTIONAL { ?statement_designer pq:P25 ?title. }   
        OPTIONAL { ?fashion_house wbt:P11 ?inception. }    
        FILTER (
        REGEX(?title, "director", "i") || 
        REGEX(?title, "head", "i") ||
        REGEX(?title, "chief","i") ||
        REGEX(?title, "founder","i")                                   
    )       
    }
    UNION
    {
    ?fashion_house p:P14 ?statement_designer.
    ?statement_designer ps:P14 ?designer
    
    OPTIONAL { ?statement_designer pq:P15 ?start_time. }
    OPTIONAL { ?statement_designer pq:P28 ?point_time. } 
    OPTIONAL { ?statement_designer pq:P16 ?end_time. }
    OPTIONAL { ?statement_designer pq:P25 ?title. }   
        OPTIONAL { ?fashion_house wbt:P11 ?inception. }    

    }
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } 
    } """


def fill_missing_end_years(df_creativeDirectors):
    """Fill missing end_year based on the next closest start_year at the same fashion house."""
    df = df_creativeDirectors.copy()
    df['start_year'] = pd.to_numeric(df['start_year'], errors='coerce')
    df['end_year'] = pd.to_numeric(df['end_year'], errors='coerce')
    df = df.groupby(['fashion_houseLabel', 'designerLabel'], as_index=False).agg({
        'start_year': 'min',  # Earliest start year
        'end_year': 'max',  # Latest end year
        'title': lambda x: ', '.join(sorted(set(x.dropna())))  # Merge unique titles
    })
    # Sort by fashion house and start_year
    df.sort_values(by=['fashion_houseLabel', 'start_year'], inplace=True, ignore_index=True)
    
    # Iterate through rows
    for i in range(len(df) - 1):  # We stop at the second last row
        if pd.isna(df.at[i, 'end_year']):  # If end_year is missing
            same_house = df.at[i, 'fashion_houseLabel'] == df.at[i+1, 'fashion_houseLabel']
            if same_house:  # If the next row is for the same fashion house
                df.at[i, 'end_year'] = df.at[i+1, 'start_year']  # Assign next closest start_year

    return df


def normalize_name(name):
    """Normalize a name by removing accents, converting to lowercase, and stripping whitespace."""
    if pd.isna(name) or not isinstance(name, str):
        return ""
    name = name.strip()
    if name.endswith("’s") or name.endswith("'s") or name.endswith("s'"):
        name = name[:-2]
    
    name = ''.join(c for c in unicodedata.normalize('NFD', name) if unicodedata.category(c) != 'Mn')
    
    return name.lower().strip()


def assign_designer(df_collection, df_creativeDirectors):
    assigned_designers = []

    # Normalize fashion house names in df_creativeDirectors
    df_creativeDirectors = df_creativeDirectors.copy()
    df_creativeDirectors["normalized_fashion_house"] = df_creativeDirectors["fashion_houseLabel"].apply(normalize_name)

    for _, row in df_collection.iterrows():
        house_original = row['fashion_house']  # Store original name
        year = row['year']

        # Normalize the fashion house name for matching
        house_normalized = normalize_name(house_original)

        # Get designers for this fashion house (using normalized name)
        designers = df_creativeDirectors[df_creativeDirectors['normalized_fashion_house'] == house_normalized].copy()

        # Convert to numeric safely without modifying the original dataframe
        designers['start_year'] = pd.to_numeric(designers['start_year'], errors='coerce')
        designers['end_year'] = pd.to_numeric(designers['end_year'], errors='coerce')

        # Case 1: Find active designers in that year
        valid_designers = designers[
            (designers['start_year'].fillna(0) <= year) & 
            ((designers['end_year'].fillna(float('inf')) >= year) | designers['end_year'].isna())
        ]

        if not valid_designers.empty:
            # Prefer the most recent designer (highest start_year)
            #selected_designer = valid_designers.sort_values(by='start_year', ascending=False).iloc[0]['designerLabel']
            selected_designers = valid_designers.sort_values(by='start_year', ascending=False)['designerLabel'].tolist()
        
        else:
            # Case 2: No direct match, assign a founder (if only one)
            founders = designers[designers['title'].str.contains('Founder', case=False, na=False)]
            if len(founders) == 1:
                selected_designers = [founders.iloc[0]['designerLabel']]
            else:
                selected_designers = []  # No valid assignment

        # Store designer info as a list with a single element (or empty list)
        #assigned_designers.append([selected_designer] if selected_designer else [])
        assigned_designers.append(selected_designers)

    # Create "designer" column in df_collection as a list with a single value per row
    df_collection['designer'] = assigned_designers

    return df_collection




def extract_designer(description, designer_dict, fashion_house):
    """
    Extract designer names from the text using spaCy NER and match them against a known designer list.
    If multiple designers are found and one matches the fashion house, only the others are kept.

    Returns:
    - A list containing **original** matched designer(s), or None if no valid match is found.
    """
    if pd.isna(description) or not isinstance(description, str):
        return []
    
    doc = nlp(description)
    
    # Extract unique PERSON entities and normalize them
    extracted_names = {normalize_name(ent.text): ent.text for ent in doc.ents if ent.label_ == "PERSON"}
    

    # Find designers in extracted names (store original names)
    matched_designers = {original for norm_name, original in extracted_names.items() if norm_name in designer_dict}

    # If multiple designers and one is the fashion house, remove the fashion house name
    if len(matched_designers) > 1 and fashion_house in matched_designers:
        matched_designers.remove(fashion_house)

    return list(matched_designers) if matched_designers else []

def refine_designer_final(row):
    # df_designers["designer_final"] = df_designers["designer"] + df_designers["designer_description"]
    # df_designers["designer_final"] = df_designers["designer_final"].apply(lambda x: list(set(x)))
    designer = set(row["designer"]) if isinstance(row["designer"], list) else set()
    designer_description = set(row["designer_description"]) if isinstance(row["designer_description"], list) else set()

    if designer & designer_description:  
        return list(set(designer))  # Rule 1: Keep only elements from "designer" if they are also in "designer_description"

    elif designer:  
        #return list(set(designer | designer_description)) # Rule 2: If "designer" is not in "designer_description", keep all elements of "designer"
        return (list(set(designer)))
    elif designer_description:  
        return list(set(designer_description)) # Rule 3: If "designer" is empty, but "designer_description" has elements, keep those
    else:
        return []  # If both are empty, return an empty list
    

from fct_extract_info_llm import *


def send_chat_prompt(prompt, model):
    client = openai.OpenAI(
            base_url="http://localhost:11434/v1" if not "gpt" in model else None,
            api_key= "ollama" if not "gpt" in model else openai_api_key)
    resp = client.chat.completions.create(
        model=model,
        temperature = 0.5 ,
        messages=[
                {"role": "system", "content": "I provide you with a text description of a fashion collection and a list of potential designers. Extract from the text the creative director of the provided collection. Generate the name of the creative director(s) in format of a list of string(s). If you can't perform the task generate an empty list"},  
                {"role": "user", "content": prompt}])
    response = resp.choices[0].message.content
    return response

def main_generate_designer(prompt,model):
    response = send_chat_prompt(prompt,model)
    response = response.replace('```','').replace('json','').replace("\n", "")
    return response

def process_and_save(df, designer_list, output_file="processed_fashion_data.json"):
    """
    Process each row in the DataFrame, extract designers, and save results to a JSON file incrementally.
    """
    processed_rows = []
    
    for index, row in df.iterrows():
        row["designer_description"] = extract_designer(row["description"], designer_list, row["fashion_house"])
        row["designer_description"] = list(set([name.replace("’s", "").replace("'s", "").strip() for name in row["designer_description"]]))
        row["designer_final"] = refine_designer_final(row)
        if len(row["designer_final"]>1) and row["description"]:
            row["designer_final"] = main_generate_designer(f"description: {row['description']}, possible designers: {row['designer_final']}", "gemma2")
        elif len(row["designer_final"]>1) and row["description"]:
            row["designer_final"] = main_generate_designer(f"description: {row['description']}", "gemma2")
        result = {"URL": row["URL"],
            "designer_description": row["designer_description"], 
            "designer": row["designer"],
            "designer_final": row["designer_final"]}
        # Append to JSON file line-by-line (ensures no data loss if interrupted)
        with open(output_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(result) + "\n")  # Append as a new JSON line
        
        processed_rows.append(result)  # Optional: Store in memory if needed
    
    return processed_rows  # Returns processed data if you want to use it in memory




def main():
    df = pd.read_parquet("data/vogue_data.parquet")
    df = df.drop_duplicates(subset="URL")
    rename_dict = {
    "Alexander Mcqueen": "Alexander McQueen",
    "Stella Mccartney": "Stella McCartney",
    "Dolce Gabbana": "Dolce & Gabbana",
    "Christian Dior": "Dior",
    "Oscar De La Renta" : "Oscar de la Renta",
    "Chloe" : "Chloé",
    "Dkny" : "DKNY",
    "Off White" : "Off-White",
    "A F Vandevorst" : "A.F. Vandevorst",
    "Viktor Rolf": "Viktor & Rolf",
    "Vpl": "VPL",
    "Comme Des Garcons": "Comme des Garçons",
    "Hermes": "Hermès",
    "Paul Joe": "Paul & Joe",
    "3 1 Phillip Lim": "3.1 Phillip Lim",
    "Bcbg Max Azria": "BCBGMAXAZRIA",
    "Saint Laurent": "Yves Saint Laurent",
    "Rag Bone" : "Rag & Bone",   
    }
    df['fashion_house'] = df['fashion_house'].replace(rename_dict)

    if os.path.exists("data/query_wikibase/query-designers-wikibase.csv"):
        designers_list = pd.read_csv("data/query_wikibase/query-designers-wikibase.csv").designerLabel
    else:
        designers_list = get_results_to_df(designer_label_query).designerLabel
    if os.path.exists("data/query_wikibase/creative_directors.csv"):
        creative_directors = pd.read_csv("data/query_wikibase/creative_directors.csv")
    else:
        creative_directors = get_results_to_df(designer_creative_directors_query)
    creative_directors = fill_missing_end_years(creative_directors)

    df = assign_designer(df, creative_directors)

    # Create a dictionary with normalized names as keys and original names as values
    designer_dict = {normalize_name(name): name for name in designers_list}

    # Ensure JSON file is empty before writing (optional)
    output_file = "data/names/fashion_show_data_all_designer.json"
    if os.path.exists(output_file):
        os.remove(output_file)

    processed_data = process_and_save(df, designer_dict, output_file)

if __name__ == "__main__":
    main()





    