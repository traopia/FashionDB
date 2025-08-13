from src_wikibase.account import *
import requests
import logging
from wikibaseintegrator import WikibaseIntegrator, datatypes,  wbi_helpers
from wikibaseintegrator.wbi_config import config
from wikibaseintegrator.wbi_exceptions import MWApiError

# List of valid language codes (can be expanded)
VALID_LANGUAGE_CODES = ['en']

def get_property_id_by_label(property_label, api_url):
    """
    Resolve the property label to its corresponding property ID from Wikibase.
    
    Args:
        property_label (str): The label of the property to search.
        api_url (str): The API URL of the target Wikibase or Wikidata.
    
    Returns:
        str: The property ID if found, otherwise None.
    """
    url = f'{api_url}/w/api.php?action=wbsearchentities&search={property_label}&language=en&type=property&format=json'
    response = requests.get(url)
    
    if response.status_code == 200:
        search_results = response.json()
        if 'search' in search_results and search_results['search']:
            # Return the first matching property ID
            return search_results['search'][0]['id']
        else:
            logging.info(f"No property found for label: {property_label}")
            return None
    else:
        logging.error(f"Failed to search for property by label in the target Wikibase. HTTP Status Code: {response.status_code}")
        return None
    

def create_property_in_wikibase(label, description, datatype):
    """ Create a new property in the Wikibase instance.
    label: str: The label of the property.
    description: str: The description of the property.
    datatype: str: The data type of the property (e.g., 'item', 'string', 'url', etc.).
    """
    if get_property_id_by_label(label, wikibase_api_url):
        print(f"Property {label} already exists in Wikibase.")
        pass
    else:
        wbi_wikibase = WikibaseIntegrator()
        # Create a new property object
        new_property = wbi_wikibase.property.new()
        # Set the label of the property (name/title of the property)
        new_property.labels.set(language='en', value=label)
        # Set the description of the property
        new_property.descriptions.set(language='en', value=description)
        # Set the data type of the property
        new_property.datatype = datatype  
        # Write the new property to the Wikibase instance
        new_property.write( mediawiki_api_url=wikibase_api_url, login=login_wikibase)
        # Print the ID of the newly created property
        print(f"Property {label} created with ID: {new_property.id}")






    

def copy_property_wikibase(property_label, equivalent_property = True):
    property_id_wikibase = get_property_id_by_label(property_label, wikibase_api_url)
    if property_id_wikibase:
        print(f"Property {property_label} has ID {property_id_wikibase} in Wikibase already.")
        pass
    else:
        try:
            # Resolve the property label to a property ID
            property_id = get_property_id_by_label(property_label, wikidata_api_url)

            print(f"Working on {property_label}")
            
            if not property_id:
                logging.error(f"Could not resolve property label {property_label} to an ID.")
                return

            # Fetch the property data from Wikidata
            url = f'https://www.wikidata.org/w/api.php?action=wbgetentities&ids={property_id}&format=json'
            response = requests.get(url)
            
            # Check for a successful response
            if response.status_code != 200:
                logging.error(f"Failed to retrieve property {property_id}. HTTP Status Code: {response.status_code}")
                return None
            
            property_data = response.json()

            if 'entities' not in property_data or property_id not in property_data['entities']:
                logging.error(f"Property {property_id} not found in response data.")
                return None
            
            # Extract necessary information
            entity = property_data['entities'][property_id]
            labels = entity.get('labels', {})
            descriptions = entity.get('descriptions', {})
            datatype = entity.get('datatype')

            if not datatype:
                logging.error(f"Property {property_id} does not have a datatype defined.")
                return None
            
            # Initialize Wikibase Integrator
            wbi = WikibaseIntegrator(login=login_wikibase)

            # Create new property in Wikibase
            new_property = wbi.property.new()

            # Set labels (e.g., only setting English here, but can easily add others)
            for lang, label in labels.items():
                if lang in VALID_LANGUAGE_CODES:  # Ensure the language code is valid
                    #logging.info(f"Setting label for language: {lang}")
                    new_property.labels.set(language=lang, value=label['value'])
                else:
                    #logging.warning(f"Skipped setting label for unrecognized language code: {lang}")
                    pass

            # Set descriptions
            for lang, description in descriptions.items():
                if lang in VALID_LANGUAGE_CODES:  # Ensure the language code is valid
                    #logging.info(f"Setting description for language: {lang}")
                    new_property.descriptions.set(language=lang, value=description['value'])
                else:
                    #logging.warning(f"Skipped setting description for unrecognized language code: {lang}")
                    pass

            # Set data type
            new_property.datatype = datatype
            if equivalent_property:
                property_url = f'https://www.wikidata.org/wiki/Property:{property_id}'
                wikidata_id_statement = datatypes.URL(value=property_url, prop_nr= get_property_id_by_label("equivalent property", wikibase_api_url))
                new_property.add_claims(wikidata_id_statement)

            # Write to Wikibase
            new_property.write(mediawiki_api_url=wikibase_api_url, login=login_wikibase)
            logging.info(f"Successfully copied property {property_id} ({property_label}) to Wikibase.")

        except MWApiError as e:
            logging.error(f"MWApiError occurred: {e}")
            if 'error' in str(e):
                logging.error(f"API Error Details: {e}")
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")