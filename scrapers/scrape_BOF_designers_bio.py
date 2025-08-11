import requests
from bs4 import BeautifulSoup
import re
import json
import pandas as pd
import fitz  # PyMuPDF


def get_designers():
    # Open the PDF file
    pdf_path = "data/names/The+Business+of+Fashion-compressed.pdf"
    #the pdf was obtained by scrolling all the way down of the page https://www.businessoffashion.com/bof500/search/?category=Designers and then printing it as a pdf
    doc = fitz.open(pdf_path)
    # List to store all the hyperlinks
    hyperlinks = []

    # Iterate through each page in the PDF
    for page_num in range(doc.page_count):
        page = doc.load_page(page_num)
        
        # Get all links on the page
        links = page.get_links()

        # Extract and store the URLs from the links
        for link in links:
            if 'uri' in link:
                hyperlinks.append(link['uri'])

    # Print all extracted hyperlinks
    designers_URLs = []
    for url in hyperlinks:
        if 'people' in url:
            designers_URLs.append(url)
        #print(url)
    designers_URLs = list(set(designers_URLs))
    # Close the document
    doc.close()
    return designers_URLs




def scrape_designer_data(url):
    """
    Scrapes designer information from the provided Business of Fashion URL.

    Parameters:
        url (str): The URL of the designer's page.

    Returns:
        dict: A dictionary containing the designer's details or None if scraping fails.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)
        
        soup = BeautifulSoup(response.text, "html.parser")
        script_tag = soup.find("script", id="fusion-metadata")
        
        if not script_tag:
            raise ValueError("Script tag with id='fusion-metadata' not found!")
        
        script_content = script_tag.string
        match = re.search(r"Fusion\.globalContent\s*=\s*({.*?});", script_content, re.DOTALL)
        
        if match:
            json_data = json.loads(match.group(1))
            return {
                "designer_name": json_data.get("title"),
                "URL": url,
                "birthdate": json_data.get("yearBorn"),
                "summary": json_data.get("summary"),
                "biography": json_data.get("editorialDescription"),
                "location": json_data.get("location", {}).get("title") if json_data.get("location") else None,
                "careers": [
                    {
                        "jobTitle": career.get("jobTitle"),
                        "timePeriod": career.get("timePeriod"),
                        "employer": career.get("profile", {}).get("title")
                    }
                    for career in json_data.get("careers", [])
                ] if json_data.get("careers") else None,
                "education": json_data.get("education"),
                "socialLinks": [
                    link.get("url")
                    for link in json_data.get("socialLinks", [])
                ] if json_data.get("socialLinks") else None
            }
        else:
            raise ValueError("Fusion.globalContent not found in script content!")
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None

def scrape_multiple_designers(designers, output_file):
    """
    Scrapes data for multiple designers and saves the results to a JSON file.

    Parameters:
        designer_urls (list): A list of designer page URLs.
        output_file (str): The path to save the JSON file.

    Returns:
        None
    """
    with open(output_file, "w", encoding="utf-8") as f:
        for designer in designers:
            if "https://" not in designer:
                url = f"https://www.businessoffashion.com/people/{designer.replace(' ','-')}"
            else:
                url = designer
            print(f"Scraping URL: {url}")
            data = scrape_designer_data(url)
            json.dump(data,f,ensure_ascii=False)
            f.write("\n")


def main():
    output_file = "data/all_designer_data_BOF.json"
    #designers = pd.read_csv("data/query_wikibase/query-designers-wikibase.csv").fashionDesignerLabel.unique().tolist()
    designers = get_designers()
    scrape_multiple_designers(designers, output_file)

if __name__ == "__main__":
    main()