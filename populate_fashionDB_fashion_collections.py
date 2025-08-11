from src_wikibase.fct_add_entities import helper_add_entities_wikibase, helper_update_entities_wikibase
import re
from src_wikibase.account import wikibase_api_url, login_wikibase
import pandas as pd
from wikibaseintegrator.wbi_helpers import remove_claims
from datetime import datetime
import os 
import spacy
import pandas as pd
#from src.queries_script.sparql_query_wikibase import *

# Load spaCy's English model with NER
nlp = spacy.load("en_core_web_sm")

class fashion_collection_to_wikibase:
    def __init__(self, fashion_house, df_properties, wikibase_api_url,print_message = False):
        self.print_message = print_message
        self.helper_add_entity= helper_add_entities_wikibase(self.print_message)
        self.helper_update_entiy = helper_update_entities_wikibase(self.print_message)
        self.wikibase_api_url = wikibase_api_url
        self.properties = self.helper_add_entity.wikibase_properties_id
        self.classes = self.helper_add_entity.classes_wikibase
        self.fetch_entity_with_label_class = self.helper_add_entity.fetch_entity_with_label_class
        self.copy_entity_wikidata_to_wikibase_label = self.helper_add_entity.copy_entity_wikidata_to_wikibase_label
        self.update_entity = self.helper_update_entiy.update_entity
        self.fashion_house = fashion_house
        self.df_properties = df_properties
        self.fashion_house_info = df_properties.loc[df_properties['fashion_house'] == fashion_house]
        self.entity = self.fetch_entity_with_label_class(fashion_house, 'fashion house', wiki="wikibase")
        self.entity_id = self.entity.id if self.entity else None



        
    def add_info(self):
        if self.entity_id:
            self.update_with_values()
        else:   
            print(f"Fashion house {self.fashion_house} not found in wikibase")
            #write to file the fashion house not found
            with open("fashion_house_not_found.txt", "a") as file:
                file.write(f"{self.fashion_house}\n")

    def extract_details_fashion_shows(self,fashion_string):
        """Extract the location, season, year, and category from a fashion show string"""
        # Define the regex pattern to capture location, season, year, and optional category
        pattern = r'^([a-zA-Z-]+-)?(pre-)?(spring|summer|fall|winter|resort|bridal)-(\d{4})(-(menswear|ready-to-wear|couture))?$'
        # Match the pattern with the string
        match = re.match(pattern, fashion_string)
        if match:
            # Extract the necessary parts
            location = match.group(1)[:-1] if match.group(1) else ""  # Remove trailing hyphen if present
            if location == "":
                season = match.group(3) or match.group(2)
            else:
                season = (match.group(2) or "") + match.group(3)
            if location == 'pre':
                location = ''
                season = 'pre-fall'
            year = match.group(4)
            category = match.group(6) if match.group(6) else ""
            

            return location, season, year, category
        else:
            return None, None, None, None



    def convert_to_wikibase_date(self,human_date: str) -> str:
        """
        Convert a human-readable date into the Wikibase format.
        
        Args:
            human_date (str): A human-readable date (e.g., "September 8, 2024").
            
        Returns:
            str: The date in Wikibase format (e.g., "+2024-09-08T00:00:00Z").
        """
        try:
            if human_date == "" or human_date is None:
                return ""
            # Parse the human-readable date
            parsed_date = datetime.strptime(human_date, "%B %d, %Y")
            
            # Convert to Wikibase format
            wikibase_date = f"+{parsed_date.strftime('%Y-%m-%dT00:00:00Z')}"
            return wikibase_date
        except ValueError as e:
            raise ValueError(f"Error parsing date: {e}. Make sure the format is 'Month Day, Year'.")
    
    def copy_and_get_show(self, show_label, reference_URL):
        show_id = self.fetch_entity_with_label_class(show_label, 'fashion season collection', wiki="wikibase", create_new_if_not_exist = True, reference_URL=reference_URL).id
        if show_id:
            location, season,year, category = self.extract_details_fashion_shows(show_label)   

            season_id = self.fetch_entity_with_label_class(season, 'fashion season', wiki="wikibase", create_new_if_not_exist = True, reference_URL=reference_URL).id if season else None
            category_id = self.fetch_entity_with_label_class(category, 'fashion show category', wiki="wikibase", create_new_if_not_exist = True, reference_URL=reference_URL).id if category else None
            location_id = self.fetch_entity_with_label_class(location, 'geographic location', wiki="wikibase", create_new_if_not_exist = True,reference_URL=reference_URL).id if location else None
            year_formatted = year + "-00-00T00:00:00Z" if year else None
            self.update_entity(show_id, self.properties["instance of"],self.classes["fashion season collection"] , type_entry = "item") 
            self.update_entity(show_id, self.properties["fashion season"],season_id , type_entry = "item") if season_id else None
            self.update_entity(show_id, self.properties["point in time"], year_formatted, type_entry = "time") if year else None
            self.update_entity(show_id, self.properties["fashion show location"], location_id, type_entry = "item") if location_id else None
            self.update_entity(show_id, self.properties["fashion show category"],category_id , type_entry = "item") if category_id else None
        return show_id

    def clean_string(self,value):
        if isinstance(value, str):
            return re.sub(r'\s+', ' ', value).strip()  # Replace any whitespace (including newlines) with a single space
        
        return value
    
    def extract_designer(self,description, designer_list, fashion_house):
        """
        Extracts designer names from text using spaCy NER and matches against a known designer list.
        
        If multiple designers are found and one matches the fashion house name, only the other designers are kept.

        Parameters:
        - description (str): Text description of a fashion collection.
        - designer_list (set): A set of known designer names.
        - fashion_house (str): The fashion house associated with the collection.

        Returns:
        - A list containing the matched designer(s), or None if no valid match is found.
        """
        if pd.isna(description) or not isinstance(description, str):
            return None  # Handle missing or non-string descriptions
        designer_set = set(designer_list)
        doc = nlp(description)
        extracted_names = {ent.text for ent in doc.ents if ent.label_ == "PERSON"}  # Extract unique PERSON entities
        
        # Find intersection with known designers
        matched_designers = extracted_names & designer_set

        if len(matched_designers) > 1 and fashion_house in matched_designers:
            matched_designers.remove(fashion_house)  # Remove the fashion house name if present

        return list(matched_designers) if matched_designers else None
    
    def split_string_nicely(self,text, max_len=2500):
        # Ensure the text is not empty
        if len(text) <= max_len:
            return [text]

        # Find the last full stop before max_len
        split_index = text.rfind('.', 0, max_len)

        # If no full stop is found, split at max_len
        if split_index == -1:
            split_index = max_len

        # Create the two parts
        part1 = text[:split_index + 1].strip()  # Include the full stop
        part2 = text[split_index + 1:].strip()  # Start from the character after the full stop

        # Recursively handle the second part if it exceeds max_len
        if len(part2) > max_len:
            return [part1] + self.split_string_nicely(part2, max_len)
        else:
            return [part1, part2]

    def update_with_values(self):
        property_id = self.properties["fashion collection"]
        all_claims_property_GUID = "|".join([claim.id for claim in self.entity.claims.get(property_id)])
        if all_claims_property_GUID:
            remove_claims(all_claims_property_GUID, self.entity_id, mediawiki_api_url=wikibase_api_url, login=login_wikibase)
        for info in self.fashion_house_info.index:
            show_label = self.fashion_house_info.show[info]
            reference_URL = self.fashion_house_info.URL[info]
            date = self.convert_to_wikibase_date(self.fashion_house_info.publish_date[info])
            show_id = self.copy_and_get_show(show_label, reference_URL)
            print(show_label)
            if show_id:
                editor_label = self.fashion_house_info.editor[info]
                editor_id = self.fetch_entity_with_label_class(editor_label, 'fashion journalist', wiki="wikibase", create_new_if_not_exist = True, reference_URL=reference_URL).id if editor_label else None
                description = self.clean_string(self.fashion_house_info.description[info]) if self.fashion_house_info.description[info] else None
                # designer = self.extract_designer(description, self.designers_list, self.fashion_house)
                # designer = designer[0] if designer else None
                # designer_id = self.fetch_entity_with_label_class(designer, 'fashion designer', wiki="wikibase", create_new_if_not_exist = False, reference_URL=reference_URL).id if designer else None
                # print(designer)
                if not description:
                    continue
                if len(description) > 2500:
                    description_formatted = self.split_string_nicely(description)
                    qualifiers_to_add = [ {'qualifier_property': self.properties["description of fashion collection"],
                                'value_qualifier': description_formatted[0],    
                                'string_qualifier': True},
                                {'qualifier_property': self.properties["description of fashion collection"],
                                'value_qualifier': description_formatted[1],
                                'string_qualifier': True},
                                {'qualifier_property': self.properties["image of fashion collection"],
                                'value_qualifier': self.fashion_house_info.image_urls[info][0] if self.fashion_house_info.image_urls[info] else None, 
                                'url_qualifier': True},
                                {'qualifier_property': self.properties["editor of fashion collection description"],
                                'value_qualifier': editor_id, 
                                'item_qualifier': True},
                                {'qualifier_property': self.properties["date of fashion collection"],
                                 'value_qualifier': date,
                                 'time_qualifier': True},
                                #  {'qualifier_property': self.properties["designer of collection"],
                                #  'value_qualifier': designer_id,
                                #  'item_qualifier': True},
                                ]
                else:    
                    qualifiers_to_add = [ {'qualifier_property': self.properties["description of fashion collection"],
                                'value_qualifier': description,    
                                'string_qualifier': True},
                                {'qualifier_property': self.properties["image of fashion collection"],
                                'value_qualifier': self.fashion_house_info.image_urls[info][0] if self.fashion_house_info.image_urls[info] else None, 
                                'url_qualifier': True},
                                {'qualifier_property': self.properties["editor of fashion collection description"],
                                'value_qualifier': editor_id, 
                                'item_qualifier': True},
                                {'qualifier_property': self.properties["date of fashion collection"],
                                 'value_qualifier': date,
                                 'time_qualifier': True},
                                # {'qualifier_property': self.properties["designer of collection"],
                                #  'value_qualifier': designer_id,
                                #  'item_qualifier': True},
                                ]
                self.update_entity(self.entity_id, property_id, show_id, type_entry = "item", reference_value=reference_URL,
                        reference_property= self.properties["reference URL"], qualifiers=qualifiers_to_add)
                self.update_entity(show_id, self.properties["fashion house X fashion collection"], self.entity_id, type_entry = "item")





def main():
    df_all = pd.read_parquet("data/vogue_data.parquet")

    #sort df_all based on fashion house
    fashion_houses = df_all.fashion_house.unique().tolist()
    for fashion_house in fashion_houses:
    #for fashion_house in fashion_houses[fashion_house_index:]:
        print("ADDING FASHION COLLECTIONS FOR", fashion_house)
        add_fashion_collection = fashion_collection_to_wikibase(fashion_house, df_all, wikibase_api_url)
        add_fashion_collection.add_info()

if __name__ == "__main__":
    main()

