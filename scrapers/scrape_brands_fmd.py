import time
from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
import os
import string
import json

# Initialize Selenium WebDriver with Chrome in headless mode
from scrape_names_fmd import initialize_driver


def scrape_fashion_shows(brand_url,driver):
    """Scrape fashion shows with pagination from a brand's page."""
    fashion_shows = []
    page = 1


    while True:
        url = f"{brand_url}shows/" if page == 1 else f"{brand_url}shows/page/{page}/"
        
        driver.get(url)
        time.sleep(3)  # Wait for the page to load
        
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        
        show_modules = soup.find_all("article", class_="PhotoModule TitleInside LinkContainer FashionWork FashionShow")
        
        if not show_modules:
            break
        
        for show in show_modules:
            show_name = show.find("div", class_="Title").get_text(strip=True)
            fashion_shows.append(show_name)
        
        page += 1

    return fashion_shows

def scrape_designers(brand_url,driver):
    """Scrape designer names from a brand's designer page."""
    designers = []
    designer_page_url = f"{brand_url}designers/"
    
    driver.get(designer_page_url)
    time.sleep(3)  # Wait for the page to load
    
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, "html.parser")
    
    designer_modules = soup.find_all("article", class_="PhotoModule TitleInside LinkContainer Designer FillImage Portrait")
    
    for designer in designer_modules:
        if designer.find("h3", itemprop="accountablePerson"):
            designer_name = designer.find("h3", itemprop="accountablePerson").get_text(strip=True)
            designers.append(designer_name)

    return designers



def scrape_about(soup):
    # Initialize the result dictionary
    result = {
        'founded_by': None,
        'belongs_to': None,
        'about': None
    }

    # Find the "About" section
    section = soup.find('section', class_='PageSection TextSection About')
    if section:
        # Get the main content of the section
        content_div = section.find('div', class_='PageSectionContent')
        if content_div:
            paragraphs = content_div.find_all('p')
            
            current_label = None
            about_text = []
            
            for p in paragraphs:
                # Check if the paragraph contains any key section title
                if p.b and p.b.string:
                    label = p.b.string.strip().lower()
                    if label == 'founded by':
                        current_label = 'founded_by'
                    elif label == 'belongs to':
                        current_label = 'belongs_to'
                    else:
                        current_label = None  # Unknown label, skip it
                else:
                    # Add the content to the appropriate label if it's defined
                    if current_label:
                        result[current_label] = p.get_text(strip=True)
                    else:
                        # If no subsection, treat as part of 'about'
                        about_text.append(p.get_text(strip=True))
            
            # If 'about' section was implicitly parsed (no labels), join its content
            result['about'] = ' '.join(about_text).strip() if about_text else None

    return result

def extract_single_field(soup, section_class, target_field):
    """
    Extract the value of a single field from a section with a given class.
    
    :param soup: BeautifulSoup object containing the HTML content.
    :param section_class: Class of the section to search for.
    :param target_field: The target field to search for (e.g., "Location", "Website").
    
    :return: The extracted value as a string or None if the field is not found.
    """
    section = soup.find("section", class_=section_class)

    if section:
        # Find all divs with the class "Data" within the section
        data_divs = section.find_all("div", class_="Data")
        
        for data_div in data_divs:
            key_div = data_div.find("div", class_="Key")
            value_div = data_div.find("div", class_="Value")

            if key_div and value_div:
                key_text = key_div.get_text(strip=True).lower()
                
                # Match the target field
                if target_field.lower() in key_text:
                    return extract_value(target_field, value_div)
    
    return None
def scrape_info(soup, target ):
    # Make a request to get the content of the web page
    address_section = soup.find("section", class_="PageSection BrandContactSummary")
    # Look for the city, country, and website in the divs
    if address_section:
        # Get the divs with the 'Data' class
        data_divs = address_section.find_all("div", class_="Data")
        if target == "city":
            for div in data_divs:
                city = div.find("div", class_="Key").get_text(strip=True)
                return city
            
        elif target == "country":
            for div in data_divs:
                country = div.find("div", class_="Value").get_text(strip=True)
                return country

        elif target == "website":
            for div in data_divs:
                key = div.find("div", class_="Key").get_text(strip=True)
                if key.lower() == "website":
                    website = div.find("div", class_="Value").find("a", href=True)['href']

                    return website.replace("//www.fashionmodeldirectory.com/go-", "https://")

def extract_value(key_text, value_div):
    """
    Extract the appropriate value based on the field (e.g., handle special cases like URL extraction).
    
    :param key_text: The key field (e.g., "website").
    :param value_div: The value div containing the data.
    
    :return: The extracted value as a string.
    """
    # Special handling for fields like website URL
    if 'website' in key_text:
        anchor_tag = value_div.find("a", href=True)
        if anchor_tag:
            return anchor_tag['href']
    
    # Default case: return the text of the value div
    return value_div.get_text(strip=True)



import re

def fix_url(url):
    # Remove any duplicate protocols at the beginning of the URL
    if type(url) == str:
        url = re.sub(r'^(https?:\/\/)+', 'https://', url)  # Replaces multiple https:// or http:// with a single https://

        # If there's no protocol, add 'https://' by default
        if not re.match(r'^https?:\/\/', url):
            url = 'https://' + url

        return url
    else:
        return url
    
def scrape_brands(output_file, names):
    """Main function to scrape brand information and their associated data."""

    driver = initialize_driver()

    # Check if JSON file already exists
    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            existing_data = [json.loads(line) for line in f]
            scraped_urls = {entry[0][1] for entry in existing_data}  # Extracted 'URL' from saved JSON
    else:
        existing_data = []
        scraped_urls = set()

    for brand_name, brand_url in zip(names['brand_name'], names['URL']):
        if brand_url in scraped_urls:
            print(f"Skipping {brand_url}, already scraped.")
            continue

        # Open the brand's URL
        driver.get(brand_url)
        print(f"Scraping {brand_name}...")

        time.sleep(5)  # Wait for the page to load
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Extract details
        city = scrape_info(soup, "city")
        country = scrape_info(soup, "country")
        website = scrape_info(soup, "website")
        website = fix_url(website)

        about_all = scrape_about(soup)
        founded_by = about_all['founded_by']
        belongs_to = about_all['belongs_to']
        about = about_all['about']

        # Extract social media links
        social_media_links = []
        social_media_section = soup.find("section", class_="PageSection OfficialSocialMedia")
        if social_media_section:
            for link in social_media_section.find_all("a", href=True):
                social_media_links.append(link['href'].strip())

        # Scrape fashion shows and designers
        fashion_shows = scrape_fashion_shows(brand_url, driver)
        designers = scrape_designers(brand_url, driver)

        # Prepare the data in the desired structure
        brand_entry = {"brand_name": brand_name, 
                       "URL": brand_url,
                       "city": city,
                       "country": country,
                       "website": website,
                       "founded_by": list(founded_by) if isinstance(founded_by, list) else list([founded_by]),
                       "belongs_to": list(belongs_to) if isinstance(belongs_to, list) else list([belongs_to]),
                       "about": about,
                       "social_media": list(social_media_links),
                       "fashion_shows": list(fashion_shows),
                       "designers": list(designers)
                       }

        with open(output_file, 'a') as f:
                json.dump(brand_entry, f)
                f.write('\n')

        # Mark URL as scraped
        scraped_urls.add(brand_url)

    driver.quit()
    print("Scraping complete!")


def main(brand_to_scrape=None, brand_index = False):
    output_file = "data/scraped_data/brand_data_fmd.json"
    names_df = pd.read_csv("data/names/brand_data_fmd_names.csv")

    if brand_index:
        index = list(names_df.brand_name).index(brand_to_scrape)
        names = names_df.iloc[index:]
    
    if brand_to_scrape and not brand_index:
        names = names_df[names_df['brand_name'] == brand_to_scrape]

    else:
        names = names_df

    scrape_brands(output_file, names)

if __name__ == "__main__":
    brand_to_scrape = None
    brand_index = False
    main(brand_to_scrape, brand_index)