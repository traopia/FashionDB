from bs4 import BeautifulSoup
import time
import pandas as pd
import string
import os
from selenium import webdriver
import argparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def scrape_number_from_page(soup):
    """Extract the number of designers from the page."""
    under_strip = soup.find("div", class_="UnderStrip")
    if under_strip:
        number_div = under_strip.find("div", class_="Primary Color_Page")
        if number_div:
            return number_div.get_text(strip=True)
    return None 



def initialize_driver():
    """Initialize the Selenium WebDriver."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode (no UI)
    chrome_options.add_argument("--no-sandbox")  # Bypass OS security model
    chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
    chrome_options.add_argument("--disable-gpu")  # Disable GPU hardware acceleration
    chrome_options.add_argument("--remote-debugging-port=9222")  # Debugging port
    driver = webdriver.Chrome(options=chrome_options)
    return driver


def scrape_names(driver, letter,csv_file, var = 'designers'):
    """Scrape designers for a specific letter.
    which_data: 'designers' or 'brands'"""
    data = []
    if var == 'designers':
        base_url = f"https://www.fashionmodeldirectory.com/{var}/search/alphabetical_order/{letter}/?start="
    elif var == 'brands':
        base_url = f"https://www.fashionmodeldirectory.com/{var}/{letter}/?start="
    start = 0

        # Load existing data if the CSV file already exists
    if os.path.exists(csv_file):
        existing_data = pd.read_csv(csv_file)
        scraped_urls = set(existing_data['URL'].tolist())
    else:
        existing_data = pd.DataFrame(columns=[f"{var}_name", "URL"])
        scraped_urls = set()


    while True:
        page_url = f"{base_url}{start}"
        driver.get(page_url)
        time.sleep(5)  # Wait for the page to load

        soup = BeautifulSoup(driver.page_source, "html.parser")
        number = scrape_number_from_page(soup)
        if not number:
            print(f"Number of {var} for letter '{letter}' not found.")
            break
        else:
            print(f"Number of {var} for letter '{letter}': {number}")

            links = soup.find_all("div", class_="Link")
            if not links:
                break

            for div in links:
                url = div.find("a", href=True)['href']
                if url.startswith("//"):
                    url = "https:" + url
                if f'{var}/{letter.lower()}' in url:
                    name = url.split("/")[-2].replace("-", " ").title()
                    print(name)
                    if url in scraped_urls:
                        print(f"Skipping {url}, already scraped.")
                        continue
                    data.append({f"{var}_name": name, "URL": url})

                    scraped_urls.add(url)
                    if data:
                        df = pd.DataFrame(data)
                        df.to_csv(csv_file, mode='a', header=not os.path.exists(csv_file), index=False)
                        data = []  # Clear the list after writing to CSV

            if start < int(number):
                start += 12
            else:
                break

def main(var):
    """Main function to execute the scraping process."""
    driver = initialize_driver()
    if var == 'designers':
        csv_file = "data/designer_data_fmd_names.csv"
        for letter in string.ascii_uppercase[16:]:
            print(f"Scraping designers starting with letter '{letter}'")
            scrape_names(driver, letter, csv_file, var)
    else:
        #for letter in string.ascii_lowercase:
        for letter in 'v':
            csv_file = "data/brand_data_fmd_names.csv"
            print(f"Scraping brands starting with letter '{letter}'")
            scrape_names(driver, letter, csv_file, var)


    driver.quit()
    print("Scraping complete!")

if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("var", help="designers or brands")
    args = argparser.parse_args()

    main(args.var)

