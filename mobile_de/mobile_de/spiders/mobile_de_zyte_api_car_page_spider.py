import json
import logging
import sys

import scrapy
from scrapy.crawler import CrawlerProcess
from dotenv import load_dotenv

sys.path.append("../..")
import pandas as pd

from inputs import custom_scrapy_settings, listing_page_crawling_framework
from mobile_de_selenium_code_prod_listing_page_func import date_start_for_log_file_name
from scrapy.utils.reactor import install_reactor

# Load environment variables
load_dotenv()

class CarPageSpider(scrapy.Spider):
    name = "car_page_spider" # Define the name of the spider
    custom_settings=custom_scrapy_settings # Define the custom settings of the spider
    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")

    # Send an initial request to the URL to be crawled
    def start_requests(self):
        # Open the JSON file containing the data
        logging.info("Opening the JSON file obtained from the Selenium script to retrieve the URLs crawled from the listing pages...")
        with open("car_page_url_list_cat_all.json", mode="r", encoding='utf-8') as f:
            data = json.load(f)
            f.close()

        # Get the list of dictionaries containing the listing page data
        if listing_page_crawling_framework == "selenium":
            df_all_car_page_urls = []
            for i in data: # Loop through every page
                for j in i: # Loop through each car listing on a specific page
                    df_all_car_page_urls.append(j)
        elif listing_page_crawling_framework == "zyte":
            df_all_car_page_urls = pd.DataFrame(data)
            df_all_car_page_urls = df_all_car_page_urls.drop_duplicates(subset="car_page_url").to_dict(orient="records")

        # Print a status message indicating the number of URLs to crawl
        logging.info(f"The total number of URLs to crawl is {len(df_all_car_page_urls)}")

        # Send GET requests to the URLs
        for url in df_all_car_page_urls:
            # Extract the values of the dictionary's keys and print a status message
            marke = url["marke"]
            modell = url["modell"]
            page_rank = url["page_rank"]
            total_num_pages = url["last_page"]
            url_to_crawl = url["car_page_url"]
            logging.info(f"Sending a request to crawl a page under {marke} {modell} with URL {url_to_crawl}")
            
            yield scrapy.Request(
                url=url_to_crawl,
                callback=self.parse,
                dont_filter=True,  # Don't filter duplicate requests
                meta={
                    "marke": url["marke"],
                    "modell": url["modell"],
                    "page_rank": page_rank,
                    "total_num_pages": total_num_pages,
                    "url_to_crawl": url_to_crawl
                }
            )

    # Get the last page of the brand-model combo and send parallel requests to all pages from the first page until the last one 
    def parse(self, response):
        logging.info(f"Received a response. Now, scraping the data of the car page under {response.meta['marke']} {response.meta['modell']} with URL {response.meta['url_to_crawl']}")

        # Extract the title
        title_1 = response.xpath("//h1[@id='ad-title']/text()").get()
        title_2 = response.xpath("//div[@class='listing-subtitle']/text()").get()
        if title_2 is not None:
            title = title_1 + " " + title_2
        else:
            title = title_1

        # Extract the form
        form = response.xpath("//div[@id='category-v']/text()").get()

        # Extract the fahrzeugzustand
        fahrzeugzustand = response.xpath("//div[@id='damageCondition-v']/text()").get()

        # Extract the leistung
        leistung = response.xpath("//div[text()='Leistung']/following-sibling::div/text()").get()
        
        # Extract the Getriebe
        getriebe = response.xpath("//div[text()='Getriebe']/following-sibling::div/text()").get()

        # Extract the Farbe
        farbe = response.xpath("//div[@id='color-v']/text()").get()

        # Extract the price
        preis = response.xpath("//span[@data-testid='prime-price']/text()").get()
        
        # Extract the Kilometer
        kilometer = response.xpath("//div[text()='Kilometerstand']/following-sibling::div/text()").get()
        
        # Extract the erstzulassung
        erstzulassung = response.xpath("//div[text()='Erstzulassung']/following-sibling::div/text()").get()

        # Extract the fahrzeughalter
        fahrzeughalter = response.xpath("//div[text()='Fahrzeughalter']/following-sibling::div/text()").get()
        
        # Extract the standort
        standort = response.xpath("//p[@id='seller-address']/text()").get()

        # Extract the vehicle description and join it
        fahrzeug_beschreibung = response.xpath("//div[@class='g-col-12 description']//text()").getall()
        if fahrzeug_beschreibung is not None:
            fahrzeug_beschreibung = "\n".join(fahrzeug_beschreibung)
        else:
            fahrzeug_beschreibung = None

        # Yield the output JSON file
        yield {
            "marke": response.meta["marke"].strip(),
            "modell": response.meta["modell"].strip(),
            "variante": "",
            "titel": title,
            "form": form,
            "fahrzeugzustand": fahrzeugzustand,
            'leistung': leistung,
            'getriebe': getriebe,
            "farbe": farbe,
            "preis": preis,
            'kilometer': kilometer,
            'erstzulassung': erstzulassung,
            'fahrzeughalter': fahrzeughalter,
            "standort": standort,
            "fahrzeugbeschreibung": fahrzeug_beschreibung,
            "url_to_crawl": response.meta["url_to_crawl"],
            "page_rank": response.meta["page_rank"],
            "total_num_pages": response.meta["total_num_pages"]
        }

# Define a function to run the car page spider
def run_car_page_spider():
    full_settings_dict = custom_scrapy_settings.copy()
    full_settings_dict.update({
        "FEEDS": {"df_all_brands_data_cat_all.json":{"format": "json", "overwrite": True, "encoding": "utf-8"}}, # Set the name of the output JSON file
        "LOG_FILE": f"mobile_logs_cat_all_{date_start_for_log_file_name}.log" # Set the name of the log file
    })
    process = CrawlerProcess(settings=full_settings_dict)
    process.crawl(CarPageSpider)
    process.start()