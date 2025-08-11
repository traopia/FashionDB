from selenium import webdriver
from bs4 import BeautifulSoup
import time
import pandas as pd
import string
import os
import ast
import json
from scrape_names_fmd import initialize_driver


def scrape_brands(designer_url,driver):
    """Scrape brand URLs from a designer's page."""
    brands = []
    brands_page_url = f"{designer_url}brands/"

    # Open the designer page with Selenium
    driver.get(brands_page_url)
    time.sleep(3)  # Wait for the page to load

    # Get the page source and parse it with BeautifulSoup
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, "html.parser")

    # Find all brand modules on the page
    brand_modules = soup.find_all("article", class_="PhotoModule TitleInside LinkContainer Brand")

    # Extract brand URLs and names
    for brand in brand_modules:
        link_tag = brand.find("a", href=True)
        if link_tag:
            brand_url = link_tag['href']
            if brand_url.startswith("//"):
                brand_url = "https:" + brand_url
            if 'brands' in brand_url:
                brands.append(brand_url.split("/")[-2].replace("-"," ").title())

    return brands

def extract_text_from_section(soup, section_class):
    """Helper function to extract text from a section with a given class."""
    section = soup.find("section", class_=section_class)
    if section:
        return section.find("div", class_="PageSectionContent").get_text(separator=" ").strip()
    return ""




def clean_who_wears_it(input_string):
    if isinstance(input_string, str):
        return [name.strip() for name in input_string.split(',') if name.strip()]
    return []

import ast  # Required for `ast.literal_eval`

def clean_brands(input_string):
    if isinstance(input_string, str):
        try:
            return ast.literal_eval(input_string)
        except (ValueError, SyntaxError):
            return []
    return input_string

def clean_perfumes(input_string):
    if isinstance(input_string, str):
        cleaned_lines = [line.replace('\t', ' ').strip() for line in input_string.split('\n') if line.strip()]
        try:
            return ast.literal_eval(str(cleaned_lines))  # To handle string eval if needed
        except (ValueError, SyntaxError):
            return cleaned_lines
    return input_string

def clean_social_media(input_string):
    if isinstance(input_string, str):
        return input_string.strip()
    return input_string

def clean_url(input_string):
    if isinstance(input_string, str):
        return [input_string]
    return input_string



def scrape_designers(output_file, names):
    """Scrape designer information and their associated brands."""

    driver = initialize_driver()

    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            existing_data = [json.loads(line) for line in f]
            scraped_urls = {entry[0][1] for entry in existing_data}  # Extracted 'URL' from saved JSON
    else:
        existing_data = []
        scraped_urls = set()

    designer_data = []

    for designer_name, designer_url in zip(names['designer_name'], names['URL']):
        if designer_url in scraped_urls:
            print(f"Skipping {designer_url}, already scraped.")
            continue
        driver.get(designer_url)
        print(designer_name)
        
        time.sleep(5)  # Wait for the page to load

        designer_page_source = driver.page_source
        designer_soup = BeautifulSoup(designer_page_source, "html.parser")

        
        biography = extract_text_from_section(designer_soup, "PageSection TextSection SplitContent About").replace("Show Full", " ").replace("the designers \n", " ").replace("\t", " ")
        the_look = extract_text_from_section(designer_soup, "PageSection TextSection TheLook")
        who_wears_it = extract_text_from_section(designer_soup, "PageSection TextSection WhoWearsIt")
        perfumes = extract_text_from_section(designer_soup, "PageSection TextSection Perfumes")

        social_media = ""
        social_media_section = designer_soup.find("section", class_="PageSection OfficialSocialMedia")
        if social_media_section:
            instagram_link = social_media_section.find("a", href=True)
            if instagram_link:
                social_media = instagram_link['href']

        brands = scrape_brands(designer_url,driver)

        designer_data = {
            "designer_name": designer_name,
            "URL": clean_url(designer_url),
            "biography": biography,
            "the_look": the_look,
            "who_wears_it": clean_who_wears_it(who_wears_it),
            "perfumes": clean_perfumes(perfumes),
            "social_media": clean_social_media(social_media),
            "brands": clean_brands(brands)
        }
        scraped_urls.add(designer_url)
        
        with open(output_file, 'a') as f:
            json.dump(designer_data, f)
            f.write('\n')
                    

    driver.quit()
    print("Scraping complete!")




def main(designer_to_scrape=None, designer_index = False):
    output_file = "data/designer_data_fmd.json"
    names_df = pd.read_csv("data/names/designer_data_fmd_names.csv")

    if designer_index:
        index = list(names_df.designer_name).index(designer_to_scrape)
        names = names_df.iloc[index:]
    
    if designer_to_scrape and not designer_index:
        names = names_df[names_df['designer_name'] == designer_to_scrape]

    else:
        names = names_df

    scrape_designers(output_file, names)

if __name__ == "__main__":
    designer_to_scrape = None
    designer_index = False
    main(designer_to_scrape, designer_index)

