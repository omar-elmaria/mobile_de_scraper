import json
import logging

import scrapy
from scrapy.crawler import CrawlerProcess
from dotenv import load_dotenv
import sys
sys.path.append("../..")
from mobile_de_selenium_code_prod_listing_page_func import date_start_for_log_file_name
from inputs import custom_scrapy_settings, change_cwd
import pandas as pd
import re
from scrapy.utils.reactor import install_reactor

# Load environment variables
load_dotenv()

### Helper functions
# Define a function that extracts the number of listings from the car page list JSON
def num_listings_extractor(json_data):
    # Extract the number of listings in raw format (181 ANgebote)
    num_listings_raw = [i["name"] for i in json_data["breadcrumbs"] if "Angebote" in i["name"]][0]
    num_listings = re.findall(pattern=r"\d+", string=num_listings_raw)[0]

    return num_listings

# Define the listing page spider
class ListingPageSpider(scrapy.Spider):
    name = "listing_page_spider" # Define the name of the spider
    custom_settings=custom_scrapy_settings # Define the custom settings of the spider
    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")

    # Change the current working directory if needed
    change_cwd()

    # Open the JSON file that contains the target URLs
    with open("target_url_list_cat_all.json", "r") as f:
        df_target_url = pd.DataFrame(json.load(f), columns=["marke", "modell", "listing_page_url"])
        f.close()

    # Send an initial request to the target URLs to extract the total number of pages per URL
    def start_requests(self):
        for row in self.df_target_url.iterrows():
            # Extract the column values
            col_vals = row[1]
            marke = col_vals["marke"]
            modell = col_vals["modell"]
            url = col_vals["listing_page_url"]

            # Log a message indicating that the total number of pages is being extracted for the marke and modell from a specific URL
            logging.info(f"Extracting the total number of pages for {marke} {modell} from URL --> {url}...")
            
            # Send the scrapy request
            yield scrapy.Request(
                url=url,
                meta={
                    "zyte_api_automap": {
                        "browserHtml": True,  
                        "productList": True,  
                        "productListOptions": {"extractFrom":"browserHtml"} 
                    },
                    "marke": marke,
                    "modell": modell,
                    "target_url": url,
                },
                callback=self.parse
            )
    
    # Parse the response
    def parse(self, response):
        # Parse the API response and calculate the number of pages belonging to the marke and modell
        productList = response.raw_api_response["productList"]
        num_pages = int(num_listings_extractor(productList)) // 21 + 1

        # Log a message indicating the number of pages for the marke and modell
        logging.info(f"Number of pages for {response.meta['marke']} {response.meta['modell']}: {num_pages}")

        # Send requests to all the pages of the marke-modell combo
        for page in range(1, num_pages + 1):
            paginated_url = response.meta["target_url"] + f"&pageNumber={page}"
            logging.info(f"Extracting the URLs of car pages of {response.meta['marke']} {response.meta['modell']} page {num_pages} from {paginated_url}...")
            yield scrapy.Request(
                url=paginated_url,
                meta={
                    "zyte_api_automap": {
                        "browserHtml": True,  
                        "productList": True,  
                        "productListOptions": {"extractFrom":"browserHtml"} 
                    },
                    "marke": response.meta["marke"],
                    "modell": response.meta["modell"],
                    "last_page": num_pages,
                    "page_rank": page,
                    "target_url": response.meta["target_url"],
                    "paginated_url": paginated_url,
                },
                callback=self.parse_listing_page
            )
    
    # Parse the response that extracts the car page URLs
    def parse_listing_page(self, response):
        # Log a message informing the user that the API response has been received and the car page URLs are being extracted
        logging.info(f"Extracting the URLs of car pages of {response.meta['marke']} {response.meta['modell']}\
        on page {response.meta['page_rank']} from {response.meta['paginated_url']}..."
        )

        # Parse the API response and extract the car page URLs
        productList = response.raw_api_response["productList"]

        # Yield the car page URLs in the format required for the car page spider
        for i in productList["products"]:
            # If "url" exists in the keys of the dictionary, yield the car page URL
            if any("url" in key for key in i.keys()):
                yield {
                    "marke": response.meta["marke"],
                    "modell": response.meta["modell"],
                    "last_page": response.meta["last_page"],
                    "page_rank": response.meta["page_rank"],
                    "car_page_url": i["url"]
                }

# Define a function to run the listing spider
def run_listing_page_spider():
    full_settings_dict = custom_scrapy_settings.copy()
    full_settings_dict.update({
        "FEEDS": {"car_page_url_list_cat_all.json":{"format": "json", "overwrite": True, "encoding": "utf-8"}}, # Set the name of the output JSON file,
        "LOG_FILE": f"mobile_logs_cat_all_{date_start_for_log_file_name}.log" # Set the name of the log file
    })
    process = CrawlerProcess(settings=full_settings_dict)
    process.crawl(ListingPageSpider)
    process.start()