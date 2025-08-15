import openai
import pandas as pd
import os
import json
from fct_extract_info_llm import *
MAX_RETRIES = 3
MODEL = "gemma2"


# Main processing function
def main_processing(df_properties, fashion_agents_todo, synthetic_text, synthetic_KG, OUTPUT_FILE, ontology, concepts, col_name, MODEL,retry_limit=MAX_RETRIES):
    if col_name == "designer_name":
        info = "biography"
    if col_name == "brand_name":
        info = "about"
    to_do = fashion_agents_todo.copy()
    retries = 0

    while len(to_do) and retries < retry_limit:
        current_batch = to_do.copy()  # Copy list for the current retry batch
        to_do = []  # Reset for potential next round
        for fashion_agent in current_batch:
            print(f"Processing {fashion_agent}, attempt {retries + 1}")
            bio = df_properties[df_properties[col_name] == fashion_agent][info].values
            try:
                generated_KG = generate_kg( bio, synthetic_text, synthetic_KG, ontology, concepts,MODEL)
                # Validate KG before saving
                if is_valid_kg(generated_KG, synthetic_KG):
                    data = {
                        "designer_name": fashion_agent,
                        "URL": df_properties[df_properties[col_name] == fashion_agent].URL.values[0],
                        "KG": str(generated_KG),#json.dumps(str(generated_KG)),
                        "model": MODEL
                    }
                    # Append row to CSV
                    with open(OUTPUT_FILE, 'a') as f:
                        json.dump(data, f)
                        f.write('\n')

                else:
                    print(f"Incomplete KG for {fashion_agent}. Retrying.")
                    bio = df_properties[df_properties[col_name] == fashion_agent][info].values
                    generated_KG = generate_kg( bio, synthetic_text, synthetic_KG,ontology, concepts,"gpt-4o-mini")
                    if is_valid_kg(generated_KG, synthetic_KG):
                        print(f"Completed KG for {fashion_agent} with gpt-4o-mini.")
                        data = {
                            "designer_name": fashion_agent,
                            "URL": df_properties[df_properties[col_name] == fashion_agent].URL.values[0],
                            "KG": str(generated_KG),
                            "model": "gpt-4o-mini"
                        }
                        # Append row to CSV
                        with open(OUTPUT_FILE, 'a') as f:
                            json.dump(data, f)
                            f.write('\n')
                    else:
                        print(f"Still incomplete KG for {fashion_agent}.")
                        to_do.append(fashion_agent)


            except Exception as e:
                print(f"Error with {fashion_agent}: {e}")
                to_do.append(fashion_agent)

        retries += 1

    if to_do:
        print("Some designers could not be processed successfully after max retries:", to_do)



def load_data(fashion_agent_type, source):
    if fashion_agent_type == "fashion house":
        col_name = "brand_name"
        if source=="fmd":
            INPUT_FILE = "data/brand_data_fmd.json"
            OUTPUT_FILE = "data/extracted_KG/extracted_KG_fmd_fashion_houses.json"
        if source == "vogue":
            INPUT_FILE = "data/brand_data_vogue.json"
            OUTPUT_FILE = "data/extracted_KG/extracted_KG_vogue_fashion_houses.json"

        ontology = """\nOntology Relations: {"founded_by": [[fashion designer,year ]],"designer_employed": [[fashion designer, start_date, end_date, occupation_title]]}"""
        concepts = """\nCONTEXT:\nOntology Concepts: fashion designer"""
        synthetic_KG = {"founded_by": [["Xyra Mondalis","1995"]],
        "designer_employed": [["Ryn Torkel", "creative director", "1993", "2002"],
        ["Fyla Brenith", "creative director", "1998", "2001"],
        ["Ozik Varnor", "chief designer", "2003", "2006"],
        ["Kivra Zondar", "creative designer", "2006", "2008"],
        ["Talon Greth", "creative designer", "2009", "2012"]]}
        synthetic_text = """The avant-garde fashion house Zondria, founded by Xyra Mondalis, is known for blending futuristic aesthetics with unconventional materials such as luminescent silk and graphene-infused fabrics. The label’s designs are celebrated for their bold geometry and exploration of color dynamics.
        Under the direction of Ryn Torkel from 1993 to 2002, the house saw rapid international expansion, opening flagship stores in major fashion capitals like Novara City, Zenithport, and Auris. Fyla Brenith took over creative duties in 1998, steering the label toward minimalism until 2001, when she left to establish her own atelier.
        In 2003, the enigmatic Ozik Varnor became the chief designer of Zondria. Despite critical acclaim for his three collections, his tenure was short-lived, ending in 2006 due to creative differences with the founder. Kivra Zondar was brought on in 2006 as creative designer but parted ways with the brand in 2008, citing personal reasons.
        To infuse fresh ideas, Talon Greth joined the house in 2009. His futuristic and experimental designs revitalized Zondria’s image, but after three years, he left in 2012 to pursue projects in wearable technology."""
        

    if fashion_agent_type == "fashion designer":
        col_name = "designer_name"
        if source=="BOF":
            INPUT_FILE = "data/scraped_data/all_designer_data_BOF.json"
            OUTPUT_FILE = "data/extracted_KG/extracted_KG_BOF_fashion_designers.json"
        if source == "fmd":
            INPUT_FILE = "data/scraped_data/designer_data_fmd.json"
            OUTPUT_FILE = "data/extracted_KG/extracted_KG_fmd_fashion_designers.json"
        if source == "wikipedia":
            INPUT_FILE = "data/scraped_data/designer_wikipedia_bio.jsonl"
            OUTPUT_FILE = "data/extracted_KG/extracted_KG_wikipedia_fashion_designers.json"
        
        ontology = """\nOntology Relations: {"educated_at": [[academic institution, start_date, end_date]], "employer": [[fashion house, title, start_date, end_date]], "work_location": [[city, start_date, end_date]], "award_received": [[fashion award, year]]}"""
        concepts = """\nCONTEXT:\nOntology Concepts: academic institution, fashion house, fashion award, city"""

        synthetic_KG = {
        "educated_at": [["Avondale Institute of Design (AID)", "1995", "1998"]],
        "employer": [
            ["Laurent Jovin", "intern", "", "1997"],
            ["Marlow Atelier", "founder", "1999", ""],
            ["The House of Fleura", "sold collection", "2000", ""],
            ["Atelier Amare", "designer", "2004", "2006"],
            ["Nouveau & Co.", "designer", "2009", ""]
        ],
        "work_location": [
            ["Willowford", "", "2006"],
            ["Celestia", "2006", ""]
        ],
        "award_received": [
            ["Arbor Fashion Laureate", "2006"]]}
        synthetic_text = """Evelyn Marlow was born in 1975 in Sunvale, Arborland. Her mother moved from Loria in the early 1970s. Evelyn grew up in the quiet town of Willowford. From a young age, she saved her allowance to buy custom pieces from boutique designers. After two rejections, Evelyn was finally accepted into the prestigious Avondale Institute of Design (AID) in 1995. While studying, she interned with Laurent Jovin, an influential designer known for his unique cuts and materials. She graduated with honors in 1998.
        In 1999, Evelyn launched her own label, Marlow Atelier, debuting her first collection at the Windora Fashion Week in 2000. Her designs quickly garnered attention, particularly from Sofia Dreyer, a lead buyer for The House of Fleura, one of the top fashion houses in the region. This led to her first big sale when The House of Fleura purchased her entire collection. With her bold, sculptural designs and a fearless use of color, Evelyn’s pieces were often accompanied by custom accessories crafted by Silvan Locke. By her fifth season in 2002, she had secured a partnership with Modevera Group, a highly regarded fashion conglomerate.
        From 2004 to 2006, Evelyn designed for the renowned brand Atelier Amare in Feronna. After spending a few years in Willowford, she moved her operations to the artistic hub of Celestia in 2006. That same year, Evelyn received the Arbor Fashion Laureate for her pioneering work with sustainable fabrics. She collaborated with Atelier Amare for several years before eventually signing a contract with Nouveau & Co. in 2009, where she continued to make waves in the fashion industry. Her 2010 fall collection, known for its modern yet timeless appeal, was highly praised at the Celestia Couture Show. Evelyn remains a leading advocate for sustainable fashion and is celebrated for her avant-garde designs that blend eco-consciousness with luxury. """

    if os.path.exists(OUTPUT_FILE):
        pass
    else:
        with open(OUTPUT_FILE, 'w') as f:
            f.write('')
    
    df_properties = pd.read_json(INPUT_FILE, lines=True)
    if fashion_agent_type == "fashion house":
        df_properties = df_properties[df_properties.about.notnull()]
        vogue = pd.read_parquet('data/vogue_data.parquet').fashion_house.unique().tolist()
        fashion_agents = list(set(df_properties[col_name].unique()) & set(vogue))
        
        fashion_agents.sort()

    if fashion_agent_type == "fashion designer":
        df_selected = df_selected[df_selected.biography.notnull()]
        fashion_agents = df_properties[col_name].unique()
        fashion_agents.sort()
    

    extracted_kg = pd.read_json(OUTPUT_FILE, lines=True)
    if extracted_kg.empty:
        fashion_agents_todo = fashion_agents
    else:
        fashion_agents_todo = [x for x in fashion_agents if x not in extracted_kg[col_name].unique()]
    
    return df_properties, fashion_agents_todo, synthetic_text, synthetic_KG, OUTPUT_FILE, ontology, concepts, col_name
    



def main():
    source = "fmd"
    fashion_agent_type = "fashion house"
    df_properties, fashion_agents_todo, synthetic_text, synthetic_KG, OUTPUT_FILE, ontology, concepts, col_name = load_data(fashion_agent_type, source)
    main_processing(df_properties, fashion_agents_todo, synthetic_text, synthetic_KG, OUTPUT_FILE, ontology, concepts, col_name, MODEL)

if __name__ == "__main__":
    main()