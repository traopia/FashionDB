import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src_wikibase.account_usernames_passwords import config
from urllib.parse import urlparse
import logging
log = logging.getLogger(__name__)
from time import sleep
import requests
helpers_session = requests.Session()
from wikibaseintegrator.wbi_helpers import get_user_agent
import pandas as pd
from string import Template
queries = False


def execute_sparql_query(query: str, prefix: str | None = None, endpoint: str | None = None, user_agent: str | None = None, max_retries: int = 1000, retry_after: int = 60) -> dict:
    """
    Execute any SPARQL query with the provided parameters.
    """

    sparql_endpoint_url = str(endpoint or config['SPARQL_ENDPOINT_URL'])
    user_agent = user_agent or (str(config['USER_AGENT']) if config['USER_AGENT'] is not None else None)

    hostname = urlparse(sparql_endpoint_url).hostname
    if hostname and hostname.endswith(('wikidata.org', 'wikipedia.org', 'wikimedia.org')) and user_agent is None:
        log.warning('WARNING: Please set a user agent if you interact with a Wikimedia Foundation instance.')

    if prefix:
        query = prefix + '\n' + query

    headers = {
        'Accept': 'application/sparql-results+json',
        'User-Agent': get_user_agent(user_agent),
        'Content-Type': 'application/sparql-query'  # Correct Content-Type
    }

    # Attempt to make the request
    for _ in range(max_retries):
        try:
            # Use 'data' instead of 'params' for the POST request to SPARQL
            response = helpers_session.post(sparql_endpoint_url, data=query, headers=headers)
        except requests.exceptions.ConnectionError as e:
            log.exception("Connection error: %s. Sleeping for %d seconds.", e, retry_after)
            sleep(retry_after)
            continue
        if response.status_code in (500, 502, 503, 504):
            log.error("Service unavailable (HTTP Code %d). Sleeping for %d seconds.", response.status_code, retry_after)
            sleep(retry_after)
            continue
        if response.status_code == 429:
            if 'retry-after' in response.headers:
                retry_after = int(response.headers['retry-after'])
            log.error("Too Many Requests (429). Sleeping for %d seconds", retry_after)
            sleep(retry_after)
            continue
        response.raise_for_status()  # Raise any non-success status code
        return response.json()  # Return the JSON result if successful

    raise Exception(f"No result after {max_retries} retries.")


def get_results_to_df( query):
    results = execute_sparql_query(query)
    df = pd.DataFrame(results["results"]["bindings"])
    df = df.map(lambda x: x['value'] if pd.notnull(x) else None)
    return df

if queries:
    from src_wikibase.fct_add_entities import wikibase_properties_id, classes_wikibase
    query_fashion_designers_template = Template("""
    PREFIX wbt: <https://fashionwiki.wikibase.cloud/prop/direct/>
    PREFIX wb: <https://fashionwiki.wikibase.cloud/entity/>
    PREFIX pq: <https://fashionwiki.wikibase.cloud/prop/qualifier/>  
    PREFIX ps: <https://fashionwiki.wikibase.cloud/prop/statement/> 
    PREFIX p: <https://fashionwiki.wikibase.cloud/prop/>            

    SELECT ?fashionDesignerLabel ?fashionDesigner WHERE {
    ?fashionDesigner wbt:$instance_of wb:$fashion_designer.

    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    } ORDER BY ?fashionDesignerLabel
    """)
    query_fashion_designers = query_fashion_designers_template.substitute(
        {
            "instance_of": wikibase_properties_id["instance of"],
            "fashion_designer": classes_wikibase["fashion designer"],

        }
    )

    query_fashion_houses_template = Template("""
    PREFIX wbt: <https://fashionwiki.wikibase.cloud/prop/direct/>
    PREFIX wb: <https://fashionwiki.wikibase.cloud/entity/>
    PREFIX pq: <https://fashionwiki.wikibase.cloud/prop/qualifier/>  
    PREFIX ps: <https://fashionwiki.wikibase.cloud/prop/statement/> 
    PREFIX p: <https://fashionwiki.wikibase.cloud/prop/>            

    SELECT ?fashionHouseLabel ?fashionHouse   WHERE {
    ?fashionHouse wbt:$instance_of wb:$fashion_house.

    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    } ORDER BY ?fashionHouseLabel
    """)
    #query_fashion_designers = query_fashion_designers_template.substitute(wikidata_properties_id["occupation"], fashion_designer = classes_wikidata["fashion designer"], grand_couturier = classes_wikidata["grand couturier"])
    query_fashion_house= query_fashion_houses_template.substitute(
        {
            "instance_of": wikibase_properties_id["instance of"],
            "fashion_house": classes_wikibase["fashion house"],

        }
    )



    query_school_template = Template("""
    PREFIX wbt: <https://fashionwiki.wikibase.cloud/prop/direct/>
    PREFIX wb: <https://fashionwiki.wikibase.cloud/entity/>
    PREFIX pq: <https://fashionwiki.wikibase.cloud/prop/qualifier/>  
    PREFIX ps: <https://fashionwiki.wikibase.cloud/prop/statement/> 
    PREFIX p: <https://fashionwiki.wikibase.cloud/prop/>            

    SELECT ?fashionSchoolLabel  WHERE {
    ?fashionSchool wbt:$instance_of wb:$academic_institution.

    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    } ORDER BY ?fashionSchoolLabel
    """)
    #query_fashion_designers = query_fashion_designers_template.substitute(wikidata_properties_id["occupation"], fashion_designer = classes_wikidata["fashion designer"], grand_couturier = classes_wikidata["grand couturier"])
    query_school = query_school_template.substitute(
        {
            "instance_of": wikibase_properties_id["instance of"],
            "academic_institution": classes_wikibase["academic institution"],
        })

    query_award_template = Template("""
    PREFIX wbt: <https://fashionwiki.wikibase.cloud/prop/direct/>
    PREFIX wb: <https://fashionwiki.wikibase.cloud/entity/>
    PREFIX pq: <https://fashionwiki.wikibase.cloud/prop/qualifier/>  
    PREFIX ps: <https://fashionwiki.wikibase.cloud/prop/statement/> 
    PREFIX p: <https://fashionwiki.wikibase.cloud/prop/>            

    SELECT ?fashionAwardLabel  WHERE {
    ?fashionAward wbt:$instance_of wb:$fashion_award.

    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    } ORDER BY ?fashionAwardLabel
    """)
    #query_fashion_designers = query_fashion_designers_template.substitute(wikidata_properties_id["occupation"], fashion_designer = classes_wikidata["fashion designer"], grand_couturier = classes_wikidata["grand couturier"])
    query_award = query_award_template.substitute(
        {
            "instance_of": wikibase_properties_id["instance of"],
            "fashion_award": classes_wikibase["fashion award"],
        })



def get_fashion_designers_wikibase(output_file):
    df_designers = get_results_to_df(query_fashion_designers)
    df_designers.to_csv(output_file, index=False)
    return get_results_to_df(query_fashion_designers)


def get_fashion_houses_wikibase(output_file):
    df_fashion_houses = get_results_to_df(query_fashion_house)
    df_fashion_houses.to_csv(output_file, index=False)
    return get_results_to_df(query_fashion_house)

def get_schools_wikibase(output_file):
    df_schools = get_results_to_df(query_school)
    df_schools.to_csv(output_file, index=False)
    return get_results_to_df(query_school)

def get_awards_wikibase(output_file):
    df_awards = get_results_to_df(query_award)
    df_awards.to_csv(output_file, index=False)
    return get_results_to_df(query_award)