from account import *
from fct_add_properties_wikibase import get_property_id_by_label
from wikibaseintegrator import datatypes,  wbi_helpers
from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator.models.descriptions import Descriptions
from wikibaseintegrator.models.labels import Labels
from wikibaseintegrator.models.aliases import Aliases
from wikibaseintegrator.models.sitelinks import Sitelinks
from wikibaseintegrator.datatypes import Item, Time, Quantity, String, URL
from wikibaseintegrator.models import Qualifiers, References, Reference, Claims
from wikibaseintegrator.wbi_exceptions import MWApiError
from wikibaseintegrator.wbi_helpers import mediawiki_api_call_helper, SearchError
#from src.sparql_queries import *
from rapidfuzz import process
import time
import ast
import requests
import pandas as pd
import re
import logging



def get_entity_id_by_label(search_string,wiki, dict_result=False) -> list:
    """
    Performs a search for entities in the Wikibase instance using labels and aliases.
    You can have more information on the parameters in the MediaWiki API help (https://www.wikidata.org/w/api.php?action=help&modules=wbsearchentities)

    :param search_string: A string which should be searched for in the Wikibase instance (labels and aliases)
    :param wiki: The wiki to search in. It can be "wikidata" or "wikibase"
    :param dict_result: If True, the result will be a list of dictionaries with the keys 'id', 'label', 'match', 'description' and 'aliases'. If False, the result will be a list of strings with the entity IDs.
    :return: A list of dictionaries or strings with the search results
    """
    if wiki == "wikidata":
        login = login_wikidata
        mediawiki_api_url = wikidata_api_url
    elif wiki == "wikibase":
        login = login_wikibase
        mediawiki_api_url = wikibase_api_url

    language = "en"
    strict_language = False

    params = {
        'action': 'wbsearchentities',
        'search': search_string,
        'language': language,
        'type': "item",
        'limit': 50,
        'format': 'json',
    }

    if strict_language:
        params.update({'strict_language': ''})

    cont_count = 0
    results = []

    while True:
        params.update({'continue': cont_count})
        search_results = mediawiki_api_call_helper(data=params, login = login, mediawiki_api_url=mediawiki_api_url, user_agent = config['USER_AGENT'])
        if search_results['success'] != 1:
            raise SearchError('Wikibase API wbsearchentities failed')

        for i in search_results['search']:
            if dict_result:
                description = i['description'] if 'description' in i else None
                aliases = i['aliases'] if 'aliases' in i else None
                results.append({
                    'id': i['id'],
                    'label': i['label'],
                    'match': i['match'],
                    'description': description,
                    'aliases': aliases
                })
            else:
                results.append(i['id'])

        if 'search-continue' not in search_results:
            break
        cont_count = search_results['search-continue']
        if cont_count >= 50:
            break
    return results

wikidata_properties_id = { "instance of": "P31", "occupation": "P106", "subclass of": "P279", 
                          "exact match":"P2888", "inception":"P571", "headquarters location":"P159",
                          "parent organization":"P749", "founded by":"P112", "owned by":"P127",
                          "industry":"P452", "country":"P17", "total revenue":"P2139", "reference URL":"P854", 
                          "official website":"P856", "date of birth":"P569", "place of birth":"P19",
                          "date of death":"P570", "country of citizenship": "P27", "sex or gender":"P21",
                          "educated at": "P69", "occupation":"P106","employer":"P108","work location":"P937",
                          "award received":"P166", "located in the administrative territorial entity": "P131"}
wikibase_properties_id = {"instance of": get_property_id_by_label("instance of", wikibase_api_url),
                    "reference URL": get_property_id_by_label("reference URL", wikibase_api_url),
                    "start time": get_property_id_by_label("start time", wikibase_api_url),
                    "end time": get_property_id_by_label("end time", wikibase_api_url),
                    "occupation title": get_property_id_by_label("occupation title", wikibase_api_url),
                    "educated at": get_property_id_by_label("educated at", wikibase_api_url),
                    "employer": get_property_id_by_label("employer", wikibase_api_url),
                    "work location": get_property_id_by_label("work location", wikibase_api_url),
                    "award received": get_property_id_by_label("award received", wikibase_api_url),
                    "point in time": get_property_id_by_label("point in time", wikibase_api_url),
                    "exact match": get_property_id_by_label("exact match", wikibase_api_url),
                    "date of birth": get_property_id_by_label("date of birth", wikibase_api_url),
                    "place of birth": get_property_id_by_label("place of birth", wikibase_api_url),
                    "date of death": get_property_id_by_label("date of death", wikibase_api_url),
                    "country of citizenship": get_property_id_by_label("country of citizenship", wikibase_api_url),
                    "occupation": get_property_id_by_label("occupation", wikibase_api_url),
                    "sex or gender": get_property_id_by_label("sex or gender", wikibase_api_url),
                    "official website": get_property_id_by_label("official website", wikibase_api_url),
                    "perfumes": get_property_id_by_label("perfumes", wikibase_api_url),
                    "who wears it": get_property_id_by_label("who wears it", wikibase_api_url),
                    "inception": get_property_id_by_label("inception", wikibase_api_url),
                    "headquarters location": get_property_id_by_label("headquarters location", wikibase_api_url),
                    "parent organization": get_property_id_by_label("parent organization", wikibase_api_url),
                    "founded by": get_property_id_by_label("founded by", wikibase_api_url),
                    "owned by": get_property_id_by_label("owned by", wikibase_api_url),
                    "industry": get_property_id_by_label("industry", wikibase_api_url),
                    "country": get_property_id_by_label("country", wikibase_api_url),
                    "total revenue": get_property_id_by_label("total revenue", wikibase_api_url),
                    "designer employed": get_property_id_by_label("designer employed", wikibase_api_url),
                    "country of origin": get_property_id_by_label("country of origin", wikibase_api_url),
                    "fashion collection": get_property_id_by_label("fashion collection", wikibase_api_url),
                    "fashion season": get_property_id_by_label("fashion season", wikibase_api_url),
                    "fashion show location": get_property_id_by_label("fashion show location", wikibase_api_url),
                    "description of fashion collection": get_property_id_by_label("description of fashion collection", wikibase_api_url),
                    "image of fashion collection": get_property_id_by_label("image of fashion collection", wikibase_api_url),
                    "editor of fashion collection description": get_property_id_by_label("editor of fashion collection description", wikibase_api_url),
                    "date of fashion collection": get_property_id_by_label("date of fashion collection", wikibase_api_url),
                    "fashion show category": get_property_id_by_label("fashion show category", wikibase_api_url),
                    "fashion house X fashion collection": get_property_id_by_label("fashion house X fashion collection", wikibase_api_url),
                    "designer of collection": get_property_id_by_label("designer of collection", wikibase_api_url)}

classes_wikidata = {"fashion designer": "Q3501317", "fashion house": "Q1941779", "business": "Q4830453",
                            "academic institution": "Q4671277", "geographic location": "Q2221906", "fashion award": "Q28928544",
                            "occupation": "Q12737077", "award": "Q618779", "concept": "Q151885", "human": "Q5", "organization": "Q43229",
                            "brand": "Q431289","lifestyle brand": "Q6545498", "privately held company":"Q1589009", "fashion label":"Q1618899",
                            "grand couturier": "Q4845479", "fashion brand": "Q1618899"}



classes_wikibase = {"fashion designer": get_entity_id_by_label("fashion designer", "wikibase")[0],
                        "fashion house": get_entity_id_by_label("fashion house", "wikibase")[0],
                        "business": get_entity_id_by_label("business", "wikibase")[0],
                        "academic institution": get_entity_id_by_label("academic institution", "wikibase")[0],
                        "geographic location": get_entity_id_by_label("geographic location", "wikibase")[0],
                        "fashion award": get_entity_id_by_label("fashion award","wikibase")[0],
                        "gender":get_entity_id_by_label("gender","wikibase")[0] , 
                        "occupation": get_entity_id_by_label("occupation","wikibase")[0],
                        "human": get_entity_id_by_label("human","wikibase")[0],
                        "organization": get_entity_id_by_label("organization","wikibase")[0],
                        "brand": get_entity_id_by_label("brand","wikibase")[0],
                        "lifestyle brand": get_entity_id_by_label("lifestyle brand","wikibase")[0],
                        "privately held company": get_entity_id_by_label("privately held company","wikibase")[0],
                        "fashion award": get_entity_id_by_label("fashion award", "wikibase")[0],
                        "fashion season": get_entity_id_by_label("fashion season", "wikibase")[0],
                        "fashion show category": get_entity_id_by_label("fashion show category", "wikibase")[0],
                        "fashion season collection": get_entity_id_by_label("fashion season collection", "wikibase")[0],
                        "fashion journalist": get_entity_id_by_label("fashion journalist", "wikibase")[0],
                        }# if get_entity_id_by_label("business", "wikibase") else None


class helper_add_entities_wikibase:
    def __init__(self, print_message = False):
        self.print_message = print_message
        self.wikidata_properties_id = wikidata_properties_id    
        self.wikibase_properties_id = wikibase_properties_id
        self.classes_wikidata = classes_wikidata
        self.classes_wikibase = classes_wikibase
        self.helper_update_entities = helper_update_entities_wikibase(self.print_message)
        self.get_entity_id_by_label = get_entity_id_by_label
      

    def get_ids(self,label, wiki):
        entity_ids = self.get_entity_id_by_label(label, wiki)
        normalized_label =  re.sub(r'\band\b', '&', label, flags=re.IGNORECASE).strip().lower().title()
        return entity_ids if entity_ids else self.get_entity_id_by_label(normalized_label, wiki)
    
    def fetch_entity_with_label_class(self, label, class_label, wiki, property_label=None, strict_search = False, create_new_if_not_exist = False, reference_URL=None):
        """
        Fetch an entity from WikiBase using its label, with a constraint that it must be an instance of a specific class (if provided).
        The function will search through all entities with the given label until it finds the one that is an instance of the specified class.
        
        Args:
            label (str): The label of the entity.
            class_label (str): The label of the class the entity must be an instance of. If None is provided, the function will return the first entity found.
            login_wikidata: Login credentials for Wiki API.
            api_url (str): The URL of the Wiki API to use.
            property_id (str): The ID of the property to check for the class_label (e.g., 'P31' for 'instance of', 'P279' for 'subclass of').
            
        Returns:
            The entity data if found and matches the 'instance of' constraint, otherwise None.
        """
        try:
            property_label = "instance of" if not property_label else property_label
            if wiki=="wikidata":
                login = login_wikidata
                api_url = wikidata_api_url
                property_id = self.wikidata_properties_id[property_label] 
                all_classes = self.classes_wikidata
                class_label_id = all_classes[class_label] if class_label else None
                wbi = wbi_wikidata

            if wiki=="wikibase":
                login = login_wikibase
                api_url = wikibase_api_url
                property_id = self.wikibase_properties_id[property_label]
                all_classes = self.classes_wikibase
                class_label_id = all_classes[class_label] if class_label else None
                wbi = wbi_wikibase
            # Initialize Wikibase Integrator
            #wbi = WikibaseIntegrator()
            # Fetch all entity IDs that match the label
            entity_ids = self.get_ids(label, wiki)
            if not entity_ids:
                label = re.sub(r'\band\b', '&', label, flags=re.IGNORECASE).strip().lower().title() 
                entity_ids = self.get_ids(label, wiki)
            if not entity_ids:
                if create_new_if_not_exist:
                    if class_label == "business" or "fashion house":
                        reference_URL = reference_URL if reference_URL else f"https://www.fashionmodeldirectory.com/brands/{label.lower().replace(' ','-')}/"
                        return self.create_new_entity(label, class_label, reference_URL= reference_URL)
                    if class_label == "fashion designer":
                        reference_URL = reference_URL if reference_URL else f"https://www.fashionmodeldirectory.com/designers/{label.lower().replace(' ','-')}/"
                        return self.create_new_entity(label, class_label, reference_URL= reference_URL)
                else:
                    return None and (logging.error(f"No entities found with label '{label}'.") if self.print_message else None)
            if class_label:    
                # Iterate over each entity ID and check if it has the 'instance of' property matching the desired class
                for entity_id in entity_ids:
                    entity = wbi.item.get(entity_id, mediawiki_api_url=api_url, login=login, user_agent = config['USER_AGENT'])
                    entity_claims = entity.claims.claims.keys()
                    property_id_claims_id = [claim.mainsnak.datavalue['value']['id'] for claim in entity.claims.claims[property_id] ] if property_id in entity_claims else None
                    if class_label_id in property_id_claims_id:
                        logging.info(f"Entity '{label}' (ID: {entity_id}) is an instance of '{class_label}'.") if self.print_message else None
                        return entity
                    if class_label =="fashion house":
                        business_id  = all_classes["business"]
                        organization_id  = all_classes["organization"]
                        brand_id = all_classes["brand"]
                        lifestyle_brand_id = all_classes["lifestyle brand"]
                        privately_held_company_id = all_classes["privately held company"]
                        if any(element in property_id_claims_id for element in [business_id, organization_id, brand_id, lifestyle_brand_id, privately_held_company_id]):
                        #if business_id in property_id_claims_id:# or organization_id in property_id_claims_id or brand_id in property_id_claims_id or lifestyle_brand_id in property_id_claims_id or privately_held_company_id in property_id_claims_id:
                            logging.info(f"Entity '{label}' (ID: {entity_id}) is an instance of '{class_label_id} or {business_id} or {organization_id} or {brand_id}'.") if self.print_message else None
                            return entity
                    if class_label =="fashion designer":
                        human_id  = all_classes["human"]
                        if human_id in property_id_claims_id:
                            logging.info(f"Entity '{label}' (ID: {entity_id}) is an instance of '{class_label_id} or {human_id}'.") if self.print_message else None
                            return entity
                    if class_label == "organization":
                        business_id  = all_classes["business"]
                        human_id  = all_classes["human"]
                        if any(element in property_id_claims_id for element in [business_id, human_id]):
                            logging.info(f"Entity '{label}' (ID: {entity_id}) is an instance of '{class_label_id} or {business_id} or {human_id}'.") if self.print_message else None
                            return entity

                    if not strict_search :
                        if class_label == "business":
                            fashion_house_id, fashion_designer_id  = all_classes["fashion house"], all_classes["fashion designer"]
                            if class_label_id or fashion_house_id or fashion_designer_id in property_id_claims_id:
                                logging.info(f"Entity '{label}' (ID: {entity_id}) is an instance of {class_label_id} '{fashion_designer_id} or {fashion_house_id}'.") if self.print_message else None
                                return entity
                    else:
                        pass
                
                if create_new_if_not_exist:
                    return self.create_new_entity(label, class_label, reference_URL= reference_URL)
                    # if class_label == "business" or "fashion house":
                    #     reference_URL = reference_URL if reference_URL else f"https://www.fashionmodeldirectory.com/brands/{label.lower().replace(' ','-')}/"
                    #     return self.create_new_entity(label, class_label, reference_URL= reference_URL)
                    # if class_label == "fashion designer":
                    #     reference_URL = reference_URL if reference_URL else f"https://www.fashionmodeldirectory.com/designers/{label.lower().replace(' ','-')}/"
                    #     return self.create_new_entity(label, class_label, reference_URL= reference_URL)
                else:
                    logging.warning(f"No entity with label '{label}' is an instance of '{class_label}'.") if self.print_message else None
                    return None
            else:
                # If no 'instance of' constraint is specified, return the first entity found
                entity = wbi.item.get(entity_ids[0], mediawiki_api_url=api_url, login=login, user_agent = config['USER_AGENT'])
                logging.info(f"Entity '{label}' (ID: {entity_ids[0]}) fetched successfully.") if self.print_message else None
                return entity
        except Exception as e:
            logging.error(f"An error occurred while fetching the entity: {e}")
            return None
        
    def create_new_entity(self, label, class_label, reference_URL=None):
        """Create a new entity if it doesn't exist, set initial properties and add reference."""
        print(f"Creating new entity '{label}'...") if self.print_message else None
        #write label to file

        entity = wbi_wikibase.item.new()
        entity.labels.set('en', label)
        source = "BOF" if "businessoffashion" in reference_URL else "FMD" if "fashionmodeldirectory" in reference_URL else "Vogue" if "vogue" in reference_URL else "fmd"
        entity.descriptions.set('en', f"{label} is a {class_label} from {source}. ")
        # if class_label == "fashion designer" and not reference_URL:
        #     reference_URL = f"https://www.fashionmodeldirectory.com/designers/{label.lower().replace(' ','-')}/"
        # elif class_label == "fashion house" and not reference_URL:
        #     reference_URL = f"https://www.fashionmodeldirectory.com/brands/{label.lower().replace(' ','-')}/"
        # reference_statement = datatypes.URL(value=reference_URL, prop_nr=self.wikibase_properties_id["reference URL"])
        # entity.claims.add(reference_statement)
        if class_label:
            class_statement = Item(value=self.classes_wikibase[class_label], prop_nr=self.wikibase_properties_id["instance of"])
            entity.claims.add(class_statement)
            if class_label == "fashion house":
                business_statement = Item(value=self.classes_wikibase["business"], prop_nr=self.wikibase_properties_id["instance of"])
                entity.claims.add(business_statement, ActionIfExists.MERGE_REFS_OR_APPEND)


        # Log entity creation
        with open("data/added_entities.txt", "a") as file:
            file.write(f"{label}\n")
        entity.write(mediawiki_api_url=wikibase_api_url, login=login_wikibase, as_new=True)
        #new_id = getattr(entity, "_BaseEntity__id", None)
        print(f"Entity {label} copied successfully.") if self.print_message else None
        return entity
    
    
    def copy_entity_wikidata_to_wikibase_label(self, label: str, class_label_wikidata: str, class_label_wikibase: str, property_id=None, add_instance_of=True,create_new_if_not_exist = False, strict_search = False, reference_URL=None, wikidata_id=False): 
        """Copy an entity from Wikidata to a new Wikibase instance and add a new property.
        Args:
            label (str): The label of the entity to copy.
            class_label_wikidata (str): The label of the 'instance of' property in Wikidata.
            class_label_wikibase (str): The label of the 'instance of' class in the new Wikibase instance.
            property_label (str): The ID of the Wikidata property to check for the class_label_wikidata (instance of, subclass of, etc.).

        Returns:
          new wikibase entity id
        """
        try:    
            entity_wikibase = self.fetch_entity_with_label_class(label, class_label_wikibase, wiki = "wikibase", property_label=property_id, strict_search=strict_search, reference_URL=reference_URL) 
            if entity_wikibase is None and strict_search is False:
                entity_wikibase = self.get_ids(label, "wikibase")[0] if self.get_ids(label, "wikibase") else None
            
            entity = self.fetch_entity_with_label_class(label, class_label_wikidata, wiki = "wikidata",property_label=property_id, strict_search=strict_search, reference_URL=reference_URL)
            if entity is None and strict_search is False:
                entity = self.get_ids(label, "wikidata")[0] if self.get_ids(label, "wikidata") else None
            en_label = entity.labels.get('en').value if entity else None
            if entity_wikibase is None and en_label is not None and strict_search is False:
                entity_wikibase = self.get_ids(en_label, "wikibase")[0] if self.get_ids(en_label, "wikibase") else None
            if entity_wikibase:
                print(f"Entity with label '{label}' already exists in the new Wikibase instance. Skipping...") if self.print_message else None
                new_id = getattr(entity_wikibase, "_BaseEntity__id", None)
                if wikidata_id:
                    wiki_id = entity.id if entity else ""
                    return new_id, wiki_id
                else:
                    return new_id

            #if not entity and not entity_wikibase:
            if not entity_wikibase and not entity and create_new_if_not_exist:
                entity = self.fetch_entity_with_label_class(label, class_label_wikibase, wiki = "wikibase", strict_search=True,create_new_if_not_exist = create_new_if_not_exist, reference_URL=reference_URL)
                #new_id = self.create_new_entity(label, class_label_wikibase, reference_URL, self.print_message)
                new_id = getattr(entity, "_BaseEntity__id", None) if entity else None
                if wikidata_id:
                    wiki_id = entity.id if entity else ""
                    return new_id, wiki_id
                else:
                    return new_id 

            if entity:
                entity_claims = entity.claims.claims.keys() if entity else None
                instances_of = [claim.mainsnak.datavalue['value']['id'] for claim in entity.claims.claims['P31'] ] if 'P31' in entity_claims else None
                entity.sitelinks.sitelinks.clear()
                en_label, en_description, en_aliases = (entity.labels.get('en').value if entity.labels.get('en') else None,
                                                        entity.descriptions.get('en').value if entity.descriptions.get('en') else None,
                                                        [str(alias) for alias in entity.aliases.get('en')] if entity.aliases.get('en') else [])
                entity.labels, entity.descriptions, entity.aliases , entity.claims = Labels(), Descriptions(), Aliases(), Claims()
                entity.labels.set('en',en_label)
                entity.descriptions.set('en',  en_description)
                entity.aliases.set('en', en_aliases) 

                if instances_of:
                    if add_instance_of:
                        for instance_of in instances_of:
                            _, instance_of_wikibase = self.copy_entity_wikidata_to_wikibase_id(instance_of, None,create_new_if_not_exist = create_new_if_not_exist) if self.copy_entity_wikidata_to_wikibase_id(instance_of, None,create_new_if_not_exist = create_new_if_not_exist) else None
                            instance_of_statement = Item(value=instance_of_wikibase, prop_nr=self.wikibase_properties_id["instance of"])
                            entity.claims.add(instance_of_statement,action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
                wikidata_id_statement = datatypes.URL(value=f'https://www.wikidata.org/wiki/{entity.id}', prop_nr=self.wikibase_properties_id["exact match"])
                entity.claims.add(wikidata_id_statement)
                # Statement about instance of
                if class_label_wikibase:
                    instance_of_statement = Item(value=self.classes_wikibase[class_label_wikibase], prop_nr=self.wikibase_properties_id["instance of"])
                    entity.claims.add(instance_of_statement, action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
                print(f"Copying entity '{label}' from Wikidata to the new Wikibase instance...") if self.print_message else None
                entity.write(mediawiki_api_url=wikibase_api_url, login=login_wikibase, as_new=True)
                new_id = getattr(entity, "_BaseEntity__id", None)
                print(f"Entity {label} copied successfully.") if self.print_message else None
                if wikidata_id:
                    wiki_id = entity.id if entity else ""
                    return new_id, wiki_id
                else:
                    return new_id


        except Exception as e:
            print(f"Failed to process entity {label}: {e}")
            return None
        
    def copy_entity_wikidata_to_wikibase_id(self,wikidata_id: str, class_label_wikibase: str, add_instance_of=True,create_new_if_not_exist = False): 
        """Copy an entity from Wikidata to a new Wikibase instance and add a new property.
        
        Args:
            wikidata_id (str): The Wikidata ID of the entity to copy.
            class_label_wikibase (str): The label of the 'instance of' class in the new Wikibase instance.
                
        Returns:
            label (str): The label of the entity copied.
            new_id (str): The ID of the entity copied in the new Wikibase instance.
                """
        try:
            if not wikidata_id:
                print("No Wikidata ID provided. Skipping...") if self.print_message else None
                return None,None
            wikidata_id = wikidata_id.split('/')[-1] if '/' in wikidata_id else wikidata_id
            entity = wbi_wikidata.item.get(wikidata_id, mediawiki_api_url=wikidata_api_url, login=login_wikidata, user_agent = config['USER_AGENT'])
            entity.sitelinks.sitelinks.clear()

            en_label, en_description, en_aliases= (entity.labels.get('en').value if entity.labels.get('en') else None,
                                                    entity.descriptions.get('en').value if entity.descriptions.get('en') else None,
                                                    [str(alias) for alias in entity.aliases.get('en')] if entity.aliases.get('en') else [],)
            if not en_label:
                print(f"Entity with ID '{wikidata_id}' has no English label. Skipping...") if self.print_message else None
                return None,None  # Skip if the entity has no English label
            
            if class_label_wikibase:
                new_id = self.fetch_entity_with_label_class(en_label, class_label_wikibase, wiki = "wikibase", strict_search=True,create_new_if_not_exist = create_new_if_not_exist).id if self.fetch_entity_with_label_class(en_label, class_label_wikibase, wiki = "wikibase", strict_search=True) else None
            else:
                new_id = self.get_entity_id_by_label(en_label, "wikibase")[0] if self.get_entity_id_by_label(en_label, "wikibase") else None
            if new_id:
                print(f"Entity '{en_label}' already exists in the new Wikibase instance. Skipping...") if self.print_message else None
                return en_label,new_id
            else:
                print(f"Copying entity '{en_label}' to the new Wikibase instance...") if self.print_message else None
            
            entity_claims = entity.claims.claims.keys() if entity else None
            instances_of = [claim.mainsnak.datavalue['value']['id'] for claim in entity.claims.claims['P31'] ] if 'P31' in entity_claims else None
            entity.labels, entity.descriptions, entity.aliases , entity.claims = Labels(), Descriptions(), Aliases(), Claims()
            entity.labels.set('en',en_label)
            entity.descriptions.set('en',  en_description)
            entity.aliases.set('en', en_aliases) 
            wikidata_id_statement = datatypes.URL(value=f'https://www.wikidata.org/wiki/{entity.id}', prop_nr=self.wikibase_properties_id["exact match"])
            entity.claims.add(wikidata_id_statement)
            if class_label_wikibase:
                instance_of_statement = Item(value=self.classes_wikibase[class_label_wikibase], prop_nr=self.wikibase_properties_id["instance of"])
                entity.claims.add(instance_of_statement,action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)

            if instances_of and add_instance_of:
                for instance_of in instances_of:
                    _, instance_of_wikibase = self.copy_entity_wikidata_to_wikibase_id(instance_of, None, add_instance_of=False) if self.copy_entity_wikidata_to_wikibase_id(instance_of, None, add_instance_of=False) else None
                    instance_of_statement = Item(value=instance_of_wikibase, prop_nr=self.wikibase_properties_id["instance of"])
                    entity.claims.add(instance_of_statement,action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)

            entity.write(mediawiki_api_url=wikibase_api_url, login=login_wikibase, as_new=True)
            new_id = getattr(entity, "_BaseEntity__id", None)
            print(f"Entity {en_label} copied successfully.") if self.print_message else None
            return en_label,new_id
        except Exception as e:
            print(f"Failed to process entity {en_label}: {e}")

    def copy_claims(self, entity, property_claims, property,reference, type_entry="item"):
        if property_claims:
            for claim in property_claims :
                if type_entry == "item":
                    instance_of_wikidata = claim.mainsnak.datavalue['value']['id'] if claim else None
                    _, instance_of_wikibase = self.copy_entity_wikidata_to_wikibase_id(instance_of_wikidata, None) if self.copy_entity_wikidata_to_wikibase_id(instance_of_wikidata, None) else None
                    if property == "educated at" or property == "employer" or property == "work location":
                        qualifier = [{'qualifier_property': self.wikibase_properties_id["start time"],
                                        'value_qualifier': claim.qualifiers._Qualifiers__qualifiers['P580'][0]._Snak__datavalue['value']['time'] if 'P580' in claim.qualifiers._Qualifiers__qualifiers else None , 
                                        'time_qualifier': True}, 
                                        {'qualifier_property': self.wikibase_properties_id["end time"], 
                                         'value_qualifier': claim.qualifiers._Qualifiers__qualifiers['P582'][0]._Snak__datavalue['value']['time'] if 'P582' in claim.qualifiers._Qualifiers__qualifiers else None, #if claim.qualifiers._Qualifiers__qualifiers['P582'][0]._Snak__datavalue['value']['time'] else None,
                                         'time_qualifier': True}] if claim.qualifiers._Qualifiers__qualifiers  else None
                        new_qualifier = self.helper_update_entities.add_qualifiers(qualifier) if qualifier else None
                        instance_of_statement = Item(value=instance_of_wikibase, prop_nr=self.wikibase_properties_id[property],references=reference, qualifiers=new_qualifier) if instance_of_wikibase else None
                    if property == "award received":
                        qualifier = [{'qualifier_property': self.wikibase_properties_id["point in time"],
                                        'value_qualifier': claim.qualifiers._Qualifiers__qualifiers['P585'][0]._Snak__datavalue['value']['time'], 
                                        'time_qualifier': True}] if claim.qualifiers._Qualifiers__qualifiers else None
                        new_qualifier = self.helper_update_entities.add_qualifiers(qualifier) if qualifier else None
                        instance_of_statement = Item(value=instance_of_wikibase, prop_nr=self.wikibase_properties_id[property],references=reference, qualifiers=new_qualifier) if instance_of_wikibase else None
                    else:
                        instance_of_statement = Item(value=instance_of_wikibase, prop_nr=self.wikibase_properties_id[property],references=reference) if instance_of_wikibase else None
                if type_entry == "time":
                    instance_of_wikibase = claim.mainsnak.datavalue['value']['time'] if claim else None
                    instance_of_statement = Time(time=instance_of_wikibase, prop_nr=self.wikibase_properties_id[property],references=reference) if instance_of_wikibase else None
                if type_entry == "url":
                    instance_of_wikibase = claim.mainsnak.datavalue['value'] if claim else None
                    instance_of_statement = URL(value=instance_of_wikibase, prop_nr=self.wikibase_properties_id[property],references=reference) if instance_of_wikibase else None
                if type_entry == "quantity":
                    if property == "total revenue":
                        qualifier = [{'qualifier_property': self.wikibase_properties_id["point in time"],
                                        'value_qualifier': claim.qualifiers._Qualifiers__qualifiers['P585'][0]._Snak__datavalue['value']['time'], 
                                        'time_qualifier': True}] if claim.qualifiers._Qualifiers__qualifiers['P585'][0]._Snak__datavalue['value']['time'] else None
                        instance_of_statement = Quantity(amount=claim.mainsnak.datavalue['value']['amount'], prop_nr=self.wikibase_properties_id[property],references=reference, qualifiers=self.helper_update_entities.add_qualifiers(qualifier)) if claim else None

                print(f"Adding claims for property '{property}'") if self.print_message else None
                entity.claims.add(instance_of_statement,action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND) if instance_of_statement else None


        
    def copy_entity_wikidata_to_wikibase_label_all_properties(self, label: str, class_label_wikidata: str, class_label_wikibase: str, entity_id_wikidata = None): 
        """Copy an entity from Wikidata to a new Wikibase instance and add a new property.
        Args:
            label (str): The label of the entity to copy.
            class_label_wikidata (str): The label of the 'instance of' property in Wikidata.
            class_label_wikibase (str): The label of the 'instance of' class in the new Wikibase instance.
            property_label (str): The ID of the Wikidata property to check for the class_label_wikidata (instance of, subclass of, etc.).

        Returns:
          new wikibase entity id
        """
        try:    
            entity_wikibase = self.fetch_entity_with_label_class(label, class_label_wikibase, wiki = "wikibase", strict_search=True) 
            print(f"Entity with label '{label}' already exists in the new Wikibase instance. Updating..") if entity_wikibase and self.print_message else None
            if entity_id_wikidata:
                entity_id_wikidata = entity_id_wikidata.split('/')[-1] if '/' in entity_id_wikidata else entity_id_wikidata
                entity = wbi_wikidata.item.get(entity_id_wikidata, mediawiki_api_url=wikidata_api_url, login=login_wikidata, user_agent = config['USER_AGENT'])
            else:
                entity = self.fetch_entity_with_label_class(label, class_label_wikidata, wiki = "wikidata",strict_search=True)        
            if entity:
                print(f"Updating entity '{label}' from Wikidata to the new Wikibase instance...") if  entity and entity_wikibase else None
                print(f"Copying entity '{label}' from Wikidata to the new Wikibase instance...") if not entity_wikibase else None
                entity_claims = entity.claims.claims.keys() if entity else None
                #instances_of_claims = [claim.mainsnak.datavalue['value']['id'] for claim in entity.claims.claims['P31'] ] if 'P31' in entity_claims else None
                instances_of_claims = entity.claims.claims[self.wikidata_properties_id['instance of']] if self.wikidata_properties_id['instance of'] in entity_claims else None
                if class_label_wikibase=="fashion house":
                    inception_claims = entity.claims.claims[self.wikidata_properties_id['inception']] if self.wikidata_properties_id['inception'] in entity_claims else None
                    headquarters_location_claims = entity.claims.claims[self.wikidata_properties_id['headquarters location']] if self.wikidata_properties_id['headquarters location'] in entity_claims else None
                    parent_organization_claims = entity.claims.claims[self.wikidata_properties_id['parent organization']] if self.wikidata_properties_id['parent organization'] in entity_claims else None
                    founded_by_claims = entity.claims.claims[self.wikidata_properties_id['founded by']] if self.wikidata_properties_id['founded by'] in entity_claims else None
                    owned_by_claims = entity.claims.claims[self.wikidata_properties_id['owned by']] if self.wikidata_properties_id['owned by'] in entity_claims else None
                    industry_claims = entity.claims.claims[self.wikidata_properties_id['industry']] if self.wikidata_properties_id['industry'] in entity_claims else None
                    country_claims = entity.claims.claims[self.wikidata_properties_id['country']] if self.wikidata_properties_id['country'] in entity_claims else None
                    official_website_claims = entity.claims.claims[self.wikidata_properties_id['official website']] if self.wikidata_properties_id['official website'] in entity_claims else None
                    total_revenue_claims = entity.claims.claims[self.wikidata_properties_id['total revenue']] if self.wikidata_properties_id['total revenue'] in entity_claims else None
                if class_label_wikibase=="fashion designer":
                    birthdate_claims = entity.claims.claims[self.wikidata_properties_id['date of birth']] if self.wikidata_properties_id['date of birth'] in entity_claims else None
                    birthplace_claims = entity.claims.claims[self.wikidata_properties_id['place of birth']] if self.wikidata_properties_id['place of birth'] in entity_claims else None
                    deathdate_claims = entity.claims.claims[self.wikidata_properties_id['date of death']] if self.wikidata_properties_id['date of death'] in entity_claims else None
                    country_of_citizenship_claims = entity.claims.claims[self.wikidata_properties_id['country of citizenship']] if self.wikidata_properties_id['country of citizenship'] in entity_claims else None
                    occupation_claims = entity.claims.claims[self.wikidata_properties_id['occupation']] if self.wikidata_properties_id['occupation'] in entity_claims else None
                    gender_claims = entity.claims.claims[self.wikidata_properties_id['sex or gender']] if self.wikidata_properties_id['sex or gender'] in entity_claims else None
                    educated_at_claims = entity.claims.claims[self.wikidata_properties_id['educated at']] if self.wikidata_properties_id['educated at'] in entity_claims else None
                    employer_claims = entity.claims.claims[self.wikidata_properties_id['employer']] if self.wikidata_properties_id['employer'] in entity_claims else None
                    work_location_claims = entity.claims.claims[self.wikidata_properties_id['work location']] if self.wikidata_properties_id['work location'] in entity_claims else None
                    award_received_claims = entity.claims.claims[self.wikidata_properties_id['award received']] if self.wikidata_properties_id['award received'] in entity_claims else None
                if class_label_wikibase == "academic institution":
                    official_website_claims = entity.claims.claims[self.wikidata_properties_id['official website']] if self.wikidata_properties_id['official website'] in entity_claims else None
                    inception_claims = entity.claims.claims[self.wikidata_properties_id['inception']] if self.wikidata_properties_id['inception'] in entity_claims else None
                    country_claims = entity.claims.claims[self.wikidata_properties_id['country']] if self.wikidata_properties_id['country'] in entity_claims else None
                    #location_claims = entity.claims.claims[self.wikidata_properties_id['located in the administrative territorial entity']] if self.wikidata_properties_id['located in the administrative territorial entity'] in entity_claims else None
                #entity.sitelinks.sitelinks.clear()
                en_sitelinks = entity.sitelinks.get('enwiki').title if entity.sitelinks.get('enwiki') else None
                en_label, en_description, en_aliases = (entity.labels.get('en').value if entity.labels.get('en') else None,
                                                        entity.descriptions.get('en').value if entity.descriptions.get('en') else None,
                                                        [str(alias) for alias in entity.aliases.get('en')] if entity.aliases.get('en') else [])
                entity.labels, entity.descriptions, entity.aliases , entity.claims, entity.sitelinks = Labels(), Descriptions(), Aliases(), Claims(), Sitelinks()
                entity.labels.set('en',en_label)  
                entitity_to_update = entity_wikibase if entity_wikibase else entity
                #entitity_to_update.sitelinks.set('enwiki', en_sitelinks)
                entitity_to_update.descriptions.set('en',  en_description)
                entitity_to_update.aliases.set('en', en_aliases)  
                exact_match_value = URL(value="https://www.wikidata.org/wiki/"+entity.id, prop_nr=self.wikibase_properties_id["exact match"])
                entitity_to_update.claims.add(exact_match_value)
                new_reference = self.helper_update_entities.add_references(self.wikibase_properties_id["reference URL"],"https://www.wikidata.org/wiki/"+entity.id )
                if class_label_wikibase=="fashion house":
                    self.copy_claims(entitity_to_update, instances_of_claims, "instance of", new_reference)
                    self.copy_claims(entitity_to_update, inception_claims, "inception", new_reference,type_entry="time", )
                    self.copy_claims(entitity_to_update, headquarters_location_claims, "headquarters location", new_reference)
                    self.copy_claims(entitity_to_update, parent_organization_claims, "parent organization", new_reference)
                    self.copy_claims(entitity_to_update, founded_by_claims, "founded by", new_reference)
                    self.copy_claims(entitity_to_update, owned_by_claims, "owned by", new_reference)
                    self.copy_claims(entitity_to_update, industry_claims, "industry", new_reference)
                    self.copy_claims(entitity_to_update, country_claims, "country", new_reference)
                    self.copy_claims(entitity_to_update, official_website_claims, "official website", new_reference,type_entry="url")
                    self.copy_claims(entitity_to_update, total_revenue_claims, "total revenue", new_reference ,type_entry="quantity")
                if class_label_wikibase=="fashion designer":
                    self.copy_claims(entitity_to_update, instances_of_claims, "instance of", new_reference)
                    self.copy_claims(entitity_to_update, birthdate_claims, "date of birth", new_reference,type_entry="time")
                    self.copy_claims(entitity_to_update, birthplace_claims, "place of birth", new_reference)
                    self.copy_claims(entitity_to_update, deathdate_claims, "date of death", new_reference,type_entry="time")
                    self.copy_claims(entitity_to_update, country_of_citizenship_claims, "country of citizenship", new_reference)
                    self.copy_claims(entitity_to_update, occupation_claims, "occupation", new_reference)
                    self.copy_claims(entitity_to_update, gender_claims, "sex or gender", new_reference)
                    self.copy_claims(entitity_to_update, educated_at_claims, "educated at", new_reference)
                    self.copy_claims(entitity_to_update, employer_claims, "employer", new_reference)
                    self.copy_claims(entitity_to_update, work_location_claims, "work location", new_reference)
                    self.copy_claims(entitity_to_update, award_received_claims, "award received", new_reference)
                if class_label_wikibase == "academic institution":
                    self.copy_claims(entitity_to_update, instances_of_claims, "instance of", new_reference)
                    self.copy_claims(entitity_to_update, inception_claims, "inception", new_reference,type_entry="time")
                    self.copy_claims(entitity_to_update, country_claims, "country", new_reference)
                    self.copy_claims(entitity_to_update, official_website_claims, "official website", new_reference,type_entry="url")
                # Statement about instance of
                if class_label_wikibase:
                    instance_of_statement = Item(value=self.classes_wikibase[class_label_wikibase], prop_nr=self.wikibase_properties_id["instance of"])
                    entitity_to_update.claims.add(instance_of_statement, action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
                if entity_wikibase:
                    print(f"Updating entity '{label}' from Wikidata to the new Wikibase instance...") if self.print_message else None
                    entitity_to_update.write(mediawiki_api_url=wikibase_api_url, login=login_wikibase, as_new=False)
                else:
                    print(f"Copying entity '{label}' from Wikidata to the new Wikibase instance...") if self.print_message else None
                    entitity_to_update.write(mediawiki_api_url=wikibase_api_url, login=login_wikibase, as_new=True)

                new_id = getattr(entitity_to_update, "_BaseEntity__id", None)
                print(f"Entity {label} copied successfully.") if self.print_message else None
                return new_id

        except Exception as e:
            print(f"Failed to process entity {label}: {e}")
            return None
        
    

class helper_update_entities_wikibase:
    def __init__(self, print_message = False):
        self.print_message = print_message
        self.reference_property = wikibase_properties_id["reference URL"]

    def add_references(self,reference_property,value_references):
        new_references = References()
        new_reference = Reference()
        if type(value_references) == str:
            value_references = [value_references]
        for value_reference in value_references:
            new_reference.add(datatypes.URL(prop_nr=reference_property, value=value_reference))
        new_references.add(new_reference)
        return new_references


    # def add_qualifiers(self,qualifiers_list):
    #     """
    #     Add multiple qualifiers to a statement.
    #     Args:
    #         qualifiers_list (list): A list of dictionaries containing 'qualifier_property', 'value_qualifier', 
    #                                 and 'time_qualifier' as keys.
    #                                 Example:
    #                                 [{'qualifier_property': 'P123', 'value_qualifier': '+2021-01-01T00:00:00Z', 'time_qualifier': True}, 
    #                                 {'qualifier_property': 'P456', 'value_qualifier': 'Some String', 'time_qualifier': False}]

    #     Returns:
    #         Qualifiers: A Qualifiers object containing all the added qualifiers.
    #     """
    #     new_qualifiers = Qualifiers()  # Initialize Qualifiers object
        
    #     for qualifier in qualifiers_list:
    #         qualifier_property = qualifier['qualifier_property']
    #         value_qualifier = qualifier['value_qualifier']
    #         time_qualifier = qualifier.get('time_qualifier', True)  # Default to time qualifier if not provided
    #         item_qualifier = qualifier.get('item_qualifier', False)  # Default to String qualifier if not provided

    #         if time_qualifier:
    #             value_qualifier.replace(" ", "") if value_qualifier and type(value_qualifier)== str else ''
    #             if "00:00:00Z" not in value_qualifier:
    #                 value_qualifier = self.extract_and_convert_year_to_wikibase_format(value_qualifier) if value_qualifier and type(value_qualifier)== str else None
    #             if value_qualifier:
    #                 new_qualifiers.add(datatypes.Time(prop_nr=qualifier_property, time=value_qualifier)) if value_qualifier else None
            
    #         if item_qualifier:
    #             # Add an Item qualifier
    #             new_qualifiers.add(datatypes.Item(prop_nr=qualifier_property, value=value_qualifier)) if value_qualifier else None
    #         else:
    #             # Add a String qualifier
    #             new_qualifiers.add(datatypes.String(prop_nr=qualifier_property, value=value_qualifier)) if value_qualifier else None

    #     return new_qualifiers
    
    def add_qualifiers(self, qualifiers_list):
        new_qualifiers = Qualifiers()  # Initialize Qualifiers object
        
        for qualifier in qualifiers_list:
            qualifier_property = qualifier['qualifier_property']
            value_qualifier = qualifier['value_qualifier']
            time_qualifier = qualifier.get('time_qualifier', False)  
            item_qualifier = qualifier.get('item_qualifier', False)  
            string_qualifier = qualifier.get('string_qualifier', False)  
            url_qualifier = qualifier.get('url_qualifier', False) 

            if time_qualifier:
                value_qualifier.replace(" ", "") if value_qualifier and type(value_qualifier)== str else ''
                if "00:00:00Z" not in value_qualifier:
                    value_qualifier = self.extract_and_convert_year_to_wikibase_format(value_qualifier) if value_qualifier and type(value_qualifier)== str else None
                if value_qualifier:
                    new_qualifiers.add(datatypes.Time(prop_nr=qualifier_property, time=value_qualifier)) if value_qualifier else None
            
            if item_qualifier:
                # Add an Item qualifier
                new_qualifiers.add(datatypes.Item(prop_nr=qualifier_property, value=value_qualifier)) if value_qualifier else None
            
            if string_qualifier:
                # Add a String qualifier
                new_qualifiers.add(datatypes.String(prop_nr=qualifier_property, value=value_qualifier)) if value_qualifier else None

            if url_qualifier:
                # Add a URL qualifier
                new_qualifiers.add(datatypes.URL(prop_nr=qualifier_property, value=value_qualifier)) if value_qualifier else None

        return new_qualifiers
        
    
    def extract_and_convert_year_to_wikibase_format(self,entry):
        # Match two-digit or four-digit numbers, optionally followed by an "s" (for decades)
        year_match = re.search(r'(\d{2})(\d{2})?s?', entry)
        if year_match:
            decade = year_match.group(1)  # Two-digit or start of a four-digit year part
            full_year = year_match.group(0)  # The full match (e.g., "1950", "50s", "02")
            
            # If it's a standalone two-digit year without "s", treat as exact year
            if len(decade) == 2 and not year_match.group(2):
                year_prefix = int(decade)
                if 50 <= year_prefix <= 99:
                    return f"+19{year_prefix}-00-00T00:00:00Z"  # E.g., "50" → "1950"
                elif 0 <= year_prefix <= 49:
                    return f"+20{year_prefix:02d}-00-00T00:00:00Z"  # E.g., "02" → "2002"
            
            # If it's a two-digit shorthand with "s" (like "50s"), expand as a decade
            elif len(decade) == 2 and 's' in full_year:
                year_prefix = int(decade)
                if 50 <= year_prefix <= 99:
                    return f"+19{year_prefix}-00-00T00:00:00Z"  # E.g., "50s" → "1950-00-00"
                elif 0 <= year_prefix <= 49:
                    return f"+20{year_prefix:02d}-00-00T00:00:00Z"  # E.g., "20s" → "2020-00-00"
            
            # If it's a full four-digit year, return it directly in Wikibase format
            elif year_match.group(2):
                return f"+{decade}{year_match.group(2)}-00-00T00:00:00Z"

        return None  # Return None if no year found
    
    def update_entity(self,entity_id, property_id, value_id,  type_entry = "item", reference_value = None, reference_property = None, qualifiers=None):
        """
        Update an entity with a new statement (property-value pair).
        
        Args:
            entity_id (str): The ID of the entity to update.
            property_id (str): The ID of the property to add a statement to.
            value_id (str): The ID of the value to add to the statement.
            is_time (bool): True if the value is a Time object, False otherwise.
            reference_value (str): The ID of the reference value to add to the statement.
            reference_property (str): The ID of the reference property to add to the statement.
            qualifiers (dict): A dictionary of qualifiers to add to the statement.

        Returns:
            None
        """
        reference_property = self.reference_property
        try:
            statements = []
            new_references = self.add_references( reference_property,reference_value) if reference_value else None 
            #if other_add_qualifier:
            new_qualifiers = self.add_qualifiers(qualifiers) if qualifiers else None
            # else:
            #     new_qualifiers = self.add_qualifiers(qualifiers) if qualifiers else None
            if type_entry == "time":
                # Create a Time statement
                time_statement= Time(time=value_id, prop_nr=property_id, references=new_references, qualifiers = new_qualifiers)
                statements.append(time_statement)
            elif type_entry == "item":
                # Create a regular Item statement
                item_statement = Item(value=value_id, prop_nr=property_id, references=new_references, qualifiers = new_qualifiers)
                statements.append(item_statement)
            elif type_entry == "quantity":
                # Create a regular String statement
                string_statement = Quantity(amount=value_id, prop_nr=property_id, references=new_references, qualifiers = new_qualifiers)
                statements.append(string_statement)
            elif type_entry == "string":
                # Create a regular String statement
                string_statement = String(value=value_id, prop_nr=property_id, references=new_references, qualifiers = new_qualifiers)
                statements.append(string_statement)
            elif type_entry == "url":
                # Create a regular String statement
                string_statement = URL(value=value_id, prop_nr=property_id, references=new_references, qualifiers = new_qualifiers)
                statements.append(string_statement)

            # Fetch the current entity data
            #entity = wbi_wikibase.item.get(entity_id)
            wbi = WikibaseIntegrator()
            entity = wbi.item.get(entity_id, mediawiki_api_url=wikibase_api_url, login=login_wikibase, user_agent = config['USER_AGENT'])

            #entity.claims.add(statements, action_if_exists=ActionIfExists.APPEND_OR_REPLACE)
            entity.claims.add(statements, action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
            # Write the updated entity back to the Wikibase
            entity.write( mediawiki_api_url=wikibase_api_url, login=login_wikibase)
            print(f"Successfully updated entity {entity_id} for property {property_id}") if self.print_message else None

        except Exception as e:
            print(f"Failed to update entity {entity_id}: {e} for property {property_id}")   


def run_wikibase_task(list_fashion_houses, index, entity_type,dict_fashion_designers_label_to_id=None, max_retries=3):
    #list_fashion_houses_index = list_fashion_houses.index(fashion_house)
    helper_add_entities = helper_add_entities_wikibase()
    wbi_wikidata = WikibaseIntegrator(login = login_wikidata)  # Reinitialize to refresh CSRF token, if needed
    wbi_wikibase = WikibaseIntegrator(login = login_wikibase)  # Reinitialize to refresh CSRF token, if needed
    for fashion_house in list_fashion_houses[index:] if index else list_fashion_houses:
        retries = 0
        while retries <= max_retries:
            try:
                print(f"Processing: {fashion_house}")
                if dict_fashion_designers_label_to_id:
                    entity_id_wikidata = dict_fashion_designers_label_to_id[fashion_house]
                    helper_add_entities.copy_entity_wikidata_to_wikibase_label_all_properties(
                    fashion_house, entity_type,entity_type,entity_id_wikidata)
                else:
                    helper_add_entities.copy_entity_wikidata_to_wikibase_label_all_properties(
                    fashion_house, entity_type,entity_type)
                break  # Exit the loop if successful

            #except wbi_exceptions.MWApiError as e:
            except Exception as e:
                if 'Invalid CSRF token' or 'writing' in str(e):
                    retries += 1
                    print(f"Encountered 'Invalid CSRF token' error for {fashion_house}. Retrying ({retries}/{max_retries})...")
                    time.sleep(2 ** retries)  # Exponential backoff
                    wbi_wikidata = WikibaseIntegrator(login = login_wikidata)  # Reinitialize to refresh CSRF token, if needed
                    wbi_wikibase = WikibaseIntegrator(login = login_wikibase)  # Reinitialize to refresh CSRF token, if needed
                    if dict_fashion_designers_label_to_id:
                        entity_id_wikidata = dict_fashion_designers_label_to_id[fashion_house]
                    helper_add_entities.copy_entity_wikidata_to_wikibase_label_all_properties(fashion_house, entity_type, entity_type,entity_id_wikidata)
                    if retries > max_retries:
                        print(f"Max retries reached for {fashion_house}. Moving to the next item.")
                        break  # Stop retrying after max_retries attempts
                else:
                    # Re-raise if it's a different MWApiError
                    raise

            except Exception as e:
                # General exception handling
                print(f"An unexpected error occurred: {e}")
                raise