import sys
from SPARQLWrapper import SPARQLWrapper, JSON
import unidecode
import pandas as pd
import requests

endpoint_url = "https://query.wikidata.org/sparql"

query_education = """SELECT ?schoolLabel WHERE {
  ?fashionDesigner wdt:P106 wd:Q3501317. # Occupation fashion designer

  # Educated at
  OPTIONAL { 
    ?fashionDesigner p:P69 ?educationStatement.
    ?educationStatement ps:P69 ?school. # Educated at institution

  }

  # Fetch labels for readability
  SERVICE wikibase:label { 
    bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".
    ?school rdfs:label ?schoolLabel.
  }
}"""

query_fashion_designers = """
# Fetch all entities that are instances of fashion designers or grand couturiers
SELECT ?designer_name WHERE {
  {
    ?fashionDesigner wdt:P106 wd:Q3501317. # occupation fashion designer
  }
  UNION
  {
    ?fashionDesigner wdt:P106 wd:Q4845479. # occupation grand couturier
  }

  # Fetch labels for readability
  SERVICE wikibase:label { 
    bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".
    ?fashionDesigner rdfs:label ?designer_name.
  }
}
"""

query_fashion_houses ="""
SELECT  ?brand_name WHERE {
  {
    ?fashionHouse wdt:P31/wdt:P279* wd:Q1941779.  # Instance or subclass of fashion house
  }
  UNION
  {
    ?fashionHouse wdt:P31/wdt:P279* wd:Q1618899.    # Instance or subclass of fashion brand
  }
    SERVICE wikibase:label { 
    bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".
    ?fashionHouse rdfs:label ?brand_name.
  }

}
"""





def get_results(endpoint_url, query):
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    # TODO adjust user agent; see https://w.wiki/CX6
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()

def get_results_to_df(endpoint_url, query, selected_list = []):
    results = get_results(endpoint_url, query)
    df = pd.DataFrame(results["results"]["bindings"])
    df = df.map(lambda x: x['value'] if pd.notnull(x) else None)

    if selected_list != []:
      df = df[df['designer_name'].isin(selected_list)]
    return df


def get_education_designers_wikidata(output_file):
    df_education_designers = get_results_to_df(endpoint_url, query_education)
    df_education_designers.to_csv(output_file, index=False)
    return df_education_designers

def get_fashion_designers_wikidata(output_file):
    df_fashion_designers = get_results_to_df(endpoint_url, query_fashion_designers)
    df_fashion_designers.to_csv(output_file, index=False)
    return df_fashion_designers

def get_fashion_houses_wikidata(output_file):
    df_fashion_houses = get_results_to_df(endpoint_url, query_fashion_houses)
    df_fashion_houses.to_csv(output_file, index=False)
    return df_fashion_houses


if __name__ == "__main__":
    get_education_designers_wikidata("data/names/school_names_designers_wikidata.csv")
    get_fashion_designers_wikidata("data/names/fashion_designers_wikidata.csv")
    get_fashion_houses_wikidata("data/names/fashion_houses_wikidata.csv")