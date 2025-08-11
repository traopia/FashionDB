import requests
import json
import re
from bs4 import BeautifulSoup
from unidecode import unidecode
import pandas as pd
import os
import sys




def designer_to_shows(designer):
    # Replace spaces, punctuations, special characters, etc., with '-' and make lowercase
    designer = designer.replace(' ', '-').replace('.', '-').replace('&', '').replace('+', '').replace('--', '-').lower()
    designer = unidecode(designer)

    # Designer URL
    URL = f"https://www.vogue.com/fashion-shows/designer/{designer}"

    # Make request with headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
    }
    r = requests.get(URL, headers=headers)

    if r.status_code != 200:
        print(f"Failed to fetch URL: {URL} (Status Code: {r.status_code})")
        return []

    # Soupify
    soup = BeautifulSoup(r.content, 'html.parser')

    # Find all show links
    show_elements = soup.find_all('a', {'data-testid': 'SummaryItemSimple'})

    if not show_elements:
        print(f"No shows found for {designer}")
        return []

    # Extract show names and links
    shows = []
    for element in show_elements:
        shows.append(element.text.strip())


    return shows

def modify_image_url(original_url):
    # Replace the width parameter in the URL for a higher-resolution image
    return original_url.replace("w_360", "w_1280")

def scrape_show_details(designer, show, all_urls=False):
    # Format designer and show names to match Vogue URL conventions
    show = unidecode(show.replace(' ', '-').lower())
    designer = unidecode(designer.replace(' ', '-').replace('.', '-').replace('&', '').replace('+', '').replace('--', '-').lower())

    # Construct the show URL
    url = f"https://www.vogue.com/fashion-shows/{show}/{designer}"
    print(f"Fetching: {url}")

    # Send request
    r = requests.get(url)
    if r.status_code != 200:
        print(f"Failed to retrieve {url}")
        return designer, show, None, None, None, None

    # Parse the page content
    soup = BeautifulSoup(r.content, 'html.parser')

    # Extract the description
    try:
        description_div = soup.find('div', class_='body__inner-container')
        description = description_div.get_text(strip=True) if description_div else None
    except Exception as e:
        print(f"Error extracting description for {url}: {e}")
        description = None

    # Extract the editor's name
    try:
        editor_span = soup.find('a', class_='BylineLink-gEnFiw')
        editor = editor_span.get_text(strip=True) if editor_span else None
    except Exception as e:
        print(f"Error extracting editor for {url}: {e}")
        editor = None

    # Extract the publish date
    try:
        date_span = soup.find('time', class_='ContentHeaderPublishDate-eIBicG')
        publish_date = date_span.get_text(strip=True) if date_span else None
    except Exception as e:
        print(f"Error extracting publish date for {url}: {e}")
        publish_date = None

    # Locate and parse JSON for image URLs
    try:
        # Locate the JSON data within the script tag
        script_tag = soup.find("script", string=re.compile(r'"runwayShowGalleries":'))
        script_content = script_tag.string if script_tag else None

        # Parse JSON data if found
        if script_content:
            json_data_match = re.search(r'"runwayShowGalleries":\s*({.*?})\s*;', script_content, re.DOTALL)
            if json_data_match:
                json_data_str = json_data_match.group(1).replace("\\u002F", "/")
                json_decoder = json.JSONDecoder()
                json_data, _ = json_decoder.raw_decode(json_data_str)

                # Extract image URLs
                galleries = json_data["galleries"]
                if all_urls:
                    image_urls = [modify_image_url(item["image"]["sources"]["sm"]["url"])
                                  for gallery in galleries for item in gallery["items"]]
                else:
                    image_urls = [modify_image_url(galleries[0]["items"][0]["image"]["sources"]["sm"]["url"])]
            else:
                image_urls = None
                print(f"No image JSON data found in {url}")
        else:
            image_urls = None
            print(f"No script tag found containing image data in {url}")
    except Exception as e:
        print(f"Error extracting image URLs for {url}: {e}")
        image_urls = None

    return designer, show, description, editor, publish_date, image_urls




def extract_details_fashion_shows(fashion_string):
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




def fashion_houses_to_be_done(out_path):
    if os.path.exists("data/fashion_show_data_all_allpics.json"):
        fashion_houses = pd.read_json("data/fashion_show_data_all_allpics.json", lines=True).fashion_house.unique().tolist()
    else:
        if os.path.exists('data/names/vogue.csv'):
            fashion_houses_vogue = pd.read_csv('data/names/vogue.csv').brand_name.unique().tolist()
        else:
            fashion_houses_vogue = all_designers_vogue()
            fashion_houses_vogue.to_csv('data/names/vogue.csv', index=False)


    if os.path.exists(out_path):
        df = pd.read_json(out_path, lines=True)
        fashion_houses_done = df.fashion_house.unique().tolist()
    else:
        fashion_houses_done = []
    
    to_be_done = list(set(fashion_houses).difference(set(fashion_houses_done)))
    to_be_done.sort()
    return to_be_done

def designer_to_shows_if_available(designer):
    if os.path.exists("data/fashion_show_data_all_allpics.json"):
        df = pd.read_json("data/fashion_show_data_all_allpics.json", lines=True)
        shows= df[df.fashion_house == designer].show.unique().tolist()
        return shows
    else:
        shows = designer_to_shows(designer)
        return shows

def main(out_path, all_urls):
    fashion_houses_of_interest = fashion_houses_to_be_done(out_path)
    for fashion_house in fashion_houses_of_interest:
        print("Scraping", fashion_house)
        fashion_house_scrape = fashion_house.lower().replace(' ', '-')
        shows = designer_to_shows_if_available(fashion_house)
        for show in shows:
            fashion_house_scrape, show, description, editor, publish_date, image_urls = scrape_show_details(fashion_house_scrape, show, all_urls=all_urls)
            location, season, year, category = extract_details_fashion_shows(show)
            data = {
                'fashion_house': fashion_house,
                'show': show,
                'URL': f"https://www.vogue.com/fashion-shows/{show}/{fashion_house_scrape}",
                'description': description,
                'editor': editor,
                'publish_date': publish_date,
                'image_urls': image_urls,
                'location': location,
                'season': season,
                'year': year,
                'category': category}
            
            with open(out_path, 'a') as f:

                json.dump(data, f)
                f.write('\n')


if __name__ == "__main__":
    out_path = "data/fashion_show_data_all_allpics.json"
    main(out_path, all_urls=True)
    df_all_pics = pd.read_json("data/fashion_show_data_all_allpics.json", lines=True)
    df_all_pics.to_parquet("data/vogue_data.parquet")

