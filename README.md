This repository stores the source code for the creation of FashionDB: a structured dataset about the recent history of fashion.\\

The data was scraped in September 2024.\\

Sources: \\
Fashion Model Directory (FMD): it's a website storing information about fashion brands and fashion designers (up until 20xx)\\
Business of Fashion (BOF): \\
Vogue: \\
Wikidata: \\
Wikipedia: \\

Procedure:\\
1. Scraping data\\
Scraped Vogue for a list of fashion houses whose collections are available, and scraped collections' informations and URLs of images of fashion collections. \\
``` scrapers/scrape_fashion_shows_vogue.py ``` --> data/vogue_data.parquet \\
Scraped FMD for information (textual and structured) about designers and fashion houses. \\
``` scrapers/scrape_names_fmd.py var "designers"``` --> \\
``` scrapers/scrape_names_fmd.py var "brands"``` --> \\
``` scrapers/scrape_brands_fmd.py``` --> \\
```scrapers/scrape_designers_fmd.py``` --> \\
Scraped BOF for information (textual and structured) about designers. \\
```scrapers/scrape_BOF_designers_bio.py``` --> data/all_designer_data_BOF.json \\
Get all entities of type fashion house, and with occupation fashion designer from Wikidata. 
```scrapers/sparql_query_wikidata.py``` -->


2. Extracting structured information from scraped texts.\\
Assign collection to fashion designers. Use the names previously scraped and the collection descriptions.\\
```extract_info/assign_designer_to_collection.py``` --> data/vogue_data.parquet\\
Extract info from biographies of designers and fashion houses using LLMs\\
```extract_info/knowledge_extraction_fashion.py``` --> data/extracted_KG \\


3. Data cleaning and preparation


4. Uploading to wikibase, host of FashionDB
Populate with the ontology and properties\\
```populate_ontology_fashionDB.ipynb``` \\
Populate with structured data\\
```populate_fashionDB.py``` \\
Populate with fashion collections\\
```populate_wikifashion_fashion_collections.py``` \\


