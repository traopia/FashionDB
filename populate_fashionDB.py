
from src_wikibase.fct_add_entities import *
import ast
import time

#from fill_in_KG_wikidata_from_wiki_fromlist import run_wikibase_task

class populate_wikibase_src:
    def __init__(self, fashion_agent, fashion_agent_type, df_properties, df_properties_extracted, wikibase_api_url,print_message = False):
        self.print_message = print_message
        self.helper_add_entities_wikibase = helper_add_entities_wikibase(self.print_message)
        self.helper_update_entities_wikibase = helper_update_entities_wikibase(self.print_message)
        self.wikibase_api_url = wikibase_api_url
        self.properties_wikibase = self.helper_add_entities_wikibase.wikibase_properties_id
        self.copy_entity_wikidata_to_wikibase_label = self.helper_add_entities_wikibase.copy_entity_wikidata_to_wikibase_label
        self.update_entity = self.helper_update_entities_wikibase.update_entity
        self.fashion_agent = fashion_agent
        if fashion_agent_type == "fashion house":
            column_name = "brand_name"
            entity_type = "fashion house"
            wikidata_type = "fashion house"
        elif fashion_agent_type == "fashion designer":
            column_name = "designer_name"
            entity_type = "fashion designer"
            wikidata_type = "human"
        self.column_name = column_name
        self.entity_type = entity_type
        self.df_properties = df_properties
        self.df_properties_extracted = df_properties_extracted
        self.fashion_agent_info = df_properties.loc[df_properties[column_name] == fashion_agent]
        self.fashion_agent_info_extracted = df_properties_extracted.loc[df_properties_extracted[column_name] == fashion_agent]
        self.reference_url = self.fashion_agent_info.URL.values[0][0] if type(self.fashion_agent_info.URL.values[0]) == list else self.fashion_agent_info.URL.values[0]

        self.entity_id, wikidata_id = self.copy_entity_wikidata_to_wikibase_label(self.fashion_agent,wikidata_type ,entity_type, strict_search=True,  add_instance_of=True, create_new_if_not_exist=True, wikidata_id=True) 
        if wikidata_id:
            dict_fashion_designers_label_to_id = {self.fashion_agent: f"https://www.wikidata.org/wiki/{wikidata_id}"}
            run_wikibase_task([fashion_agent],0,entity_type,dict_fashion_designers_label_to_id)


    def _copy_and_get_value(self,val_wikidata, class_label_wikidata, class_label_wikibase, add_instance_of = False, create_new_if_not_exist = False):
        value_id = self.copy_entity_wikidata_to_wikibase_label(val_wikidata,class_label_wikidata, class_label_wikibase, add_instance_of=add_instance_of, strict_search=True, create_new_if_not_exist=create_new_if_not_exist, reference_URL=self.reference_url)
        return value_id
      
    
    def extract_names(self,col_label):
        KG = self.fashion_agent_info_extracted.KG.values[0]
        # Check if the key exists and has a valid list format
        if col_label in KG and isinstance(KG[col_label], list):
            # Extract the first element from each sub-list under the specified key
            return [entry[0] for entry in KG[col_label] if entry]
        if col_label in KG and KG[col_label] == [[]]:
            return []
        if col_label in KG and KG[col_label] == None:
            return []
        else:
            # Return an empty list if the key is not found or the value isn't a list
            return []

    def _extract_values(self, col_label,extracted_info):
        if extracted_info:
            value_labels = self.extract_names(col_label)
        else:
            value_labels = self.fashion_agent_info[col_label].values.tolist() if self.fashion_agent_info[col_label].values.any() else []
            value_labels = value_labels[0] if type(value_labels[0]) == list else value_labels

        return value_labels
    
    def extract_qualifiers(self,col_label, value_label):
        KG = self.fashion_agent_info_extracted.KG.values.all()
        if col_label in KG and isinstance(KG[col_label], list):
            for entry in KG[col_label]:
                if entry and entry[0] == value_label:
                    if entry is None:
                        return None
                    if len(entry)==1:
                        return None
                    if len(entry) == 2:
                        year = entry[-1] if entry[-1] else ''
                        qualifiers = [{'qualifier_property': self.properties_wikibase["point in time"],
                                    'value_qualifier': year,
                                    'time_qualifier': True}]
                        return qualifiers
                    
                    if len(entry) > 2:
                        start_date = entry[-2] if entry[-2] else ''
                        end_date = entry[-1] if entry[-1] else ''
                        if end_date.lower() == "present":
                            end_date = "2025"
                        if self.entity_type == "fashion house":
                            title = entry[-3] if col_label == "designer_employed" and len(entry) > 3 else ''
                        if self.entity_type == "fashion designer":
                            title = entry[-3] if col_label=="employer" and len(entry) > 3 else ''
                        if title:
                            qualifiers = [{'qualifier_property': self.properties_wikibase["start time"],
                                        'value_qualifier': start_date,
                                        'time_qualifier': True},
                                        {'qualifier_property': self.properties_wikibase["end time"],
                                        'value_qualifier': end_date,
                                        'time_qualifier': True},
                                        {'qualifier_property': self.properties_wikibase["occupation title"],
                                    'value_qualifier': title ,
                                    'string_qualifier': True}] 
                        else:
                            qualifiers = [{'qualifier_property': self.properties_wikibase["start time"],
                                        'value_qualifier': start_date,
                                        'time_qualifier': True},
                                        {'qualifier_property': self.properties_wikibase["end time"],
                                        'value_qualifier': end_date,
                                        'time_qualifier': True}]
                        return qualifiers


    def _update_with_values(self, property_label,col_label, class_label_wikidata, class_label_wikibase, type_entry = "item",qualifiers_to_add=None,  create_new_if_not_exist = False, add_instance_of = False, extracted_info = False):
        property_id = self.properties_wikibase[property_label]
        value_labels = self._extract_values(col_label,extracted_info)
        if value_labels:
            for value_label in value_labels:
                if type_entry=="url":
                    if value_label:
                        self.update_entity(self.entity_id, property_id, value_label, reference_value=self.reference_url, qualifiers=None, type_entry = type_entry) 
                if type_entry == "string":
                    if value_label:
                        self.update_entity(self.entity_id, property_id, value_label, reference_value=self.reference_url, qualifiers=None, type_entry = type_entry) 
                if type_entry == "item":
                    value_id = self._copy_and_get_value(value_label, class_label_wikidata, class_label_wikibase, create_new_if_not_exist, add_instance_of)
                    if value_id:
                        qualifiers_to_add = self.extract_qualifiers(col_label, value_label)# if qualifiers_to_add is None else qualifiers_to_add
                        reference_url = [self.reference_url,self.fashion_agent_info_extracted.model.values[0]] if extracted_info else self.reference_url
                        self.update_entity(entity_id = self.entity_id, property_id=property_id,value_id=value_id,reference_value= reference_url, qualifiers = qualifiers_to_add)
                        if property_label == "founded by":
                            self.update_entity(value_id, self.properties_wikibase["instance of"],self.helper_add_entities_wikibase.classes_wikibase["fashion designer"], "item", reference_value=self.reference_url)
                            self.update_entity(value_id, self.properties_wikibase["employer"],self.entity_id, "item", reference_value=self.reference_url, qualifiers= [{'qualifier_property': self.properties_wikibase["occupation title"],'value_qualifier': "founder",'string_qualifier': True}])    
        else:
            print(f"No values found for {col_label}")
            pass


    def add_info_fashion_houses_FMD(self):
        # self.add_founded_by()
        # self.add_owned_by()
        # self.add_designer_employed()
        # self.add_country_brand()
        #extracted
        self.add_founded_by(extracted_info = True)
        self.add_designer_employed(extracted_info=True)



    def add_info_designers_FMD(self):
        #extracted
        self.add_school()
        self.add_employer()
        self.add_workLocation()
        self.add_awards()

        self.add_official_website()
        self.add_founded()
        self.add_perfumes()
        self.add_who_wears_it()


    def add_info_designers_BOF(self):
        self.add_country_designer()
        self.add_birth_date()
        self.add_social_media_BOF()
        self.add_career_BOF()
        self.add_education_BOF()

        self.add_school()
        self.add_employer()
        self.add_workLocation()
        self.add_awards()
    

    # fashion houses FMD

    def add_founded_by(self, extracted_info = False):
        self._update_with_values(property_label="founded by", col_label="founded_by",class_label_wikidata="human",class_label_wikibase="fashion designer", type_entry="item", create_new_if_not_exist=True, add_instance_of=True, extracted_info=extracted_info)

    def add_owned_by(self):
        self._update_with_values(property_label="owned by", col_label="belongs_to",class_label_wikidata="organization",class_label_wikibase="organization", type_entry="item", create_new_if_not_exist=True, add_instance_of=True)# or self._update_with_values("owned by", "belongs_to","business","business", type_entry="item") or self._update_with_values("owned by", "belongs_to","human","human", type_entry="item")

    def add_designer_employed(self, extracted_info = False):
        self._update_with_values(property_label = "designer employed", col_label="designers",class_label_wikidata="human",class_label_wikibase="fashion designer", type_entry="item", create_new_if_not_exist=True, add_instance_of=True, extracted_info=extracted_info)

    def add_country_brand(self):
        self._update_with_values(property_label="country of origin", col_label="country",class_label_wikidata=None,class_label_wikibase="geographic location", type_entry="item")
        self._update_with_values(property_label="headquarters location", col_label="city",class_label_wikidata=None,class_label_wikibase="geographic location", type_entry="item")


    # designer FMD
    def add_official_website(self):
        self._update_with_values(property_label="official website",col_label = "social_media",class_label_wikidata= None, class_label_wikibase=None, type_entry = "url")
        #if column website exists in the dataframe
        if "website" in self.df_properties.columns and self.df_properties["website"].values.any():
            self._update_with_values(property_label="official website",col_label = "website",class_label_wikidata= None, class_label_wikibase=None, type_entry = "url")

    def add_founded(self):
        qualifiers_to_add = [{'qualifier_property': self.properties_wikibase["occupation title"],
                                    'value_qualifier': "founder",
                                    'string_qualifier': True}]
        self._update_with_values(property_label="employer", col_label="brands", class_label_wikidata="business",class_label_wikibase="business", type_entry="item",qualifiers_to_add= qualifiers_to_add, add_instance_of = True)

    def add_perfumes(self):
        self._update_with_values(property_label="perfumes", col_label="perfumes", class_label_wikidata=None,class_label_wikibase=None, type_entry="string")

    def add_who_wears_it(self):
        self._update_with_values(property_label="who wears it", col_label="who_wears_it",class_label_wikidata="human",class_label_wikibase="human" ,type_entry="item")

    #extracted KG designers FMD
    def add_school(self):
        self._update_with_values(property_label="educated at",col_label ="educated_at", class_label_wikidata=None,class_label_wikibase="academic institution", extracted_info = True, type_entry = "item")

    def add_employer(self):
        self._update_with_values(property_label="employer",col_label="employer",class_label_wikidata="fashion house", class_label_wikibase="fashion house", extracted_info = True, type_entry = "item")

    def add_workLocation(self):
        self._update_with_values(property_label="work location",col_label="work_location",class_label_wikidata=None, class_label_wikibase="geographic location", extracted_info = True, type_entry = "item")
                
    def add_awards(self):
        self._update_with_values(property_label="award received",col_label="award_received",class_label_wikidata="fashion award", class_label_wikibase="fashion award", extracted_info = True, type_entry = "item", create_new_if_not_exist=True)

    #extracted KG designers BOF 


    def add_country_designer(self):
        self._update_with_values(property_label="country of citizenship",col_label="location",class_label_wikidata=None,class_label_wikibase="geographic location", type_entry="item")

    def add_birth_date(self):
        birth_date = self.fashion_agent_info.birthdate.values[0]
        if birth_date:
            formatted_date = self.helper_update_entities_wikibase.extract_and_convert_year_to_wikibase_format(str(birth_date))
            self.update_entity(self.entity_id, self.properties_wikibase["date of birth"], formatted_date, type_entry = "time", reference_value=self.reference_url)


    def add_social_media_BOF(self):
        self._update_with_values(property_label="official website",col_label="socialLinks",class_label_wikidata=None,class_label_wikibase=None, type_entry="url")

    def add_career_BOF(self):
        careers = self.fashion_agent_info.careers.values[0]
        if careers:
            for career in careers:
                employer = career['employer']
                employer_id = self.copy_entity_wikidata_to_wikibase_label(employer, None, "fashion house", create_new_if_not_exist = True, reference_URL=self.reference_url)
                if career['timePeriod']:
                    time_period = career['timePeriod'].split(" - ")
                    start_date = time_period[0] if len(time_period) > 0 else ''
                    end_date = time_period[1] if len(time_period) > 1 else ''
                else:
                    start_date, end_date = '', ''
                qualifers = [{'qualifier_property': self.properties_wikibase["start time"],
                        'value_qualifier': start_date,
                        'time_qualifier': True},
                        {'qualifier_property': self.properties_wikibase["end time"],
                        'value_qualifier': end_date,
                        'time_qualifier': True},
                        {'qualifier_property': self.properties_wikibase["occupation title"],
                     'value_qualifier': career['jobTitle'] ,
                     'string_qualifier': True}]
                self.update_entity(self.entity_id, self.properties_wikibase["employer"], employer_id, type_entry = "item", reference_value=[self.reference_url], qualifiers=qualifers)

    
    def add_education_BOF(self):
        educations = self.fashion_agent_info.education.values[0]
        if educations:
            for education in educations:
                school = education['profile']['title']
                school_id = self.copy_entity_wikidata_to_wikibase_label(school, None, "academic institution", create_new_if_not_exist = True)
                #start_date, end_date = education['timePeriod'].split(" - ") if education['timePeriod'] else ('','')
                if education['timePeriod']:
                    time_period = education['timePeriod'].split(" - ")
                    start_date = time_period[0] if len(time_period) > 0 else ''
                    end_date = time_period[1] if len(time_period) > 1 else ''
                else:
                    start_date, end_date = '', ''
                qualifers = [{'qualifier_property': self.properties_wikibase["start time"],
                        'value_qualifier': start_date,
                        'time_qualifier': True},
                        {'qualifier_property': self.properties_wikibase["end time"],
                        'value_qualifier': end_date,
                        'time_qualifier': True}]
                self.update_entity(self.entity_id, self.properties_wikibase["educated at"], school_id, type_entry = "item", reference_value=[self.reference_url], qualifiers=qualifers)



def main(fashion_agent_type, BOF, one_to_do, name_index):

    if fashion_agent_type == "fashion designer":
        df_properties = pd.read_json("data/designer_data_fmd.json", lines = True)
        df_properties_extracted = pd.read_json("data/extracted_KG/extracted_KG_fmd_fashion_designers.json", lines = True)

        if BOF:
            df_properties = pd.read_json("data/all_designer_data_BOF.json", lines = True)
            df_properties_extracted = pd.read_json("data/extracted_KG/extracted_KG_BOF_bio.json", lines = True)


    elif fashion_agent_type == "fashion house":
        df_properties = pd.read_json("data/brand_data_fmd.json", lines = True)
        df_properties_extracted = pd.read_json("data/extracted_KG/extracted_KG_fmd_fashion_houses.json", lines = True)
        vogue_brands = pd.read_csv("data/names/vogue.csv").brand_name.tolist()
        df_properties_extracted = df_properties_extracted[df_properties_extracted.brand_name.isin(vogue_brands)]

    df_properties_extracted["KG"] = df_properties_extracted["KG"].apply(lambda x: ast.literal_eval(x))
    df_properties_extracted["model"] = df_properties_extracted["model"].fillna("https://ollama.com/library/gemma2")
    df_properties_extracted["model"] = df_properties_extracted["model"].apply(lambda x: "https://ollama.com/library/gemma2" if x == "gemma2" else "https://platform.openai.com/docs/models#gpt-4o-mini" if x == "gpt-4o-mini" else x)

    all_fashion_agents = df_properties_extracted.designer_name.unique().tolist() if fashion_agent_type == "fashion designer" else df_properties_extracted.brand_name.unique().tolist()
    all_fashion_agents.sort()
    
    index_fashion_agents = all_fashion_agents.index(name_index)  if name_index in all_fashion_agents else None
    
    to_do = [all_fashion_agents[index_fashion_agents]] if one_to_do else all_fashion_agents[index_fashion_agents:] if index_fashion_agents else all_fashion_agents

    for fashion_agent in to_do:
        print("Adding info for:", fashion_agent)
        add_info = populate_wikibase_src(fashion_agent, fashion_agent_type, df_properties, df_properties_extracted, wikibase_api_url, print_message = True)
        if fashion_agent_type == "fashion house":
            add_info.add_info_fashion_houses_FMD()
        elif fashion_agent_type == "fashion designer" and BOF == False:
            add_info.add_info_designers_FMD()
        elif fashion_agent_type == "fashion designer" and BOF == True:
            add_info.add_info_designers_BOF()

if __name__ == "__main__":
    fashion_agent_type = "fashion_designer"
    BOF = True
    one_to_do = True
    name_index = "Pierpaolo Piccioli"

    main(fashion_agent_type, BOF, one_to_do, name_index)