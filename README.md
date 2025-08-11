This repository stores the source code for the creation of FashionDB: a structured dataset about the recent history of fashion.<br/>

The data was scraped in September 2024.<br/>

Sources: <br/>
Fashion Model Directory (FMD): it's a website storing information about fashion brands and fashion designers (up until 20xx)<br/>
Business of Fashion (BOF): <br/>
Vogue: <br/>
Wikidata: <br/>
Wikipedia: <br/>

Procedure:<br/>
1. Scraping data<br/>
Scraped Vogue for a list of fashion houses whose collections are available, and scraped collections' informations and URLs of images of fashion collections. <br/>
``` scrapers/scrape_fashion_shows_vogue.py ``` --> data/vogue_data.parquet <br/>
Scraped FMD for information (textual and structured) about designers and fashion houses. <br/>
``` scrapers/scrape_names_fmd.py var "designers"``` --> <br/>
``` scrapers/scrape_names_fmd.py var "brands"``` --> <br/>
``` scrapers/scrape_brands_fmd.py``` --> <br/>
```scrapers/scrape_designers_fmd.py``` --> <br/>
Scraped BOF for information (textual and structured) about designers. <br/>
```scrapers/scrape_BOF_designers_bio.py``` --> data/all_designer_data_BOF.json <br/>
Get all entities of type fashion house, and with occupation fashion designer from Wikidata. 
```scrapers/sparql_query_wikidata.py``` -->


2. Extracting structured information from scraped texts.<br/>
Assign collection to fashion designers. Use the names previously scraped and the collection descriptions.<br/>
```extract_info/assign_designer_to_collection.py``` --> data/vogue_data.parquet<br/>
Extract info from biographies of designers and fashion houses using LLMs<br/>
```extract_info/knowledge_extraction_fashion.py``` --> data/extracted_KG <br/>


3. Data cleaning and preparation


4. Uploading to wikibase, host of FashionDB
Populate with the ontology and properties<br/>
```populate_ontology_fashionDB.ipynb``` <br/>
Populate with structured data<br/>
```populate_fashionDB.py``` <br/>
Populate with fashion collections<br/>
```populate_wikifashion_fashion_collections.py``` <br/>


