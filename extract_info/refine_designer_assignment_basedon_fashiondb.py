import pandas as pd
import os

from sparql_query_wikibase import get_results_to_df

import unicodedata
import json
from fct_extract_info_llm import *






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
    df_collection['designer_wikibase'] = assigned_designers

    return df_collection




def main():
    df = pd.read_parquet("data/vogue_data.parquet")
    if os.path.exists("data/query_wikibase/creative_directors.csv"):
        creative_directors = pd.read_csv("data/query_wikibase/creative_directors.csv")
    else:
        creative_directors = get_results_to_df(designer_creative_directors_query)

    creative_directors.to_csv("data/extracted_KG/creative_directors_wikibase.csv")
    creative_directors = fill_missing_end_years(creative_directors)

    df = assign_designer(df, creative_directors)

    df.to_parquet("exp.parquet")

if __name__ == "__main__":
    main()





    