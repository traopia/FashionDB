
import pandas as pd
import os
import json
from extract_info.sparql_query_wikibase import *


import wikipediaapi

def check_wikipedia_page(name):
    """Check if a Wikipedia page exists for a person."""
    user_agent = "YourCustomUserAgent/1.0 (YourEmail@example.com)"  # Specify your custom user agent
    wiki_wiki = wikipediaapi.Wikipedia('en', headers={'User-Agent': user_agent})
    page = wiki_wiki.page(name)
    return page.exists()

def get_people_with_wikipedia(people_list):
    """Return a list of people who have a Wikipedia page."""
    people_with_wiki = []
    for person in people_list:
        if check_wikipedia_page(person):
            print(f"{person} has a Wikipedia page.")
            people_with_wiki.append(person)
        else:
            print(f"{person} does not have a Wikipedia page.")
    return people_with_wiki

def get_results_to_df( query):
    results = execute_sparql_query(query)
    df = pd.DataFrame(results["results"]["bindings"])
    df = df.map(lambda x: x['value'] if pd.notnull(x) else None)
    return df


def get_wikipedia_bio(designer_name):
    user_agent = "YourCustomUserAgent/1.0 (YourEmail@example.com)"  # Specify your custom user agent
    wiki_wiki = wikipediaapi.Wikipedia('en', headers={'User-Agent': user_agent})
    
    page = wiki_wiki.page(designer_name)
    
    if page.exists():
        return page.text
    else:
        return None


designer_name_fmd = pd.read_csv("data/names/designer_data_fmd_names.csv").designer_name.unique().tolist()
if os.path.exists('data/designer_wikipedia_bio.jsonl'):
    wikibase_designers = pd.read_json('data/designer_wikipedia_bio.jsonl', lines=True).designer_name.unique().tolist()
else:
    wikibase_designers = get_results_to_df(query_fashion_designers).to_csv("data/query_wikibase/query-designers-wikibase.csv", index=False)
designers_not_in_fmd = [x for x in wikibase_designers if x not in designer_name_fmd]

with open('data/designer_wikipedia_bio.jsonl', 'a') as file:
    for designer in designers_not_in_fmd:
        wiki_bio = get_wikipedia_bio(designer)
        if wiki_bio:
            wiki_bio = wiki_bio if ("fashion" in wiki_bio or "designer" in wiki_bio) else None
            if wiki_bio and "may refer to" in wiki_bio:
                designer_new = designer + " (designer)"
                wiki_bio = get_wikipedia_bio(designer_new)
        #remove the newline character
        if wiki_bio:
            entry = {
                "designer_name": designer,
                "biography": wiki_bio,
                "URL": "https://en.wikipedia.org/wiki/" + designer.replace(" ", "_")
            }
            # Write each entry as a JSON object on a new line
            file.write(json.dumps(entry, ensure_ascii=False) + '\n')