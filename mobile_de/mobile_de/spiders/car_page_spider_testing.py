import scrapy
from scrapy.utils.response import open_in_browser
from scrapy.shell import inspect_response
from dotenv import load_dotenv
from scraper_api import ScraperAPIClient
import os


# Load environment variables
load_dotenv()

# Instantiate a ScraperAPI client
client = ScraperAPIClient(api_key=os.getenv(key="SCRAPER_API_KEY"))

# Define custom settings for the spider
custom_settings_dict = {
    "FEED_EXPORT_ENCODING": "utf-8", # UTF-8 deals with all types of characters
    "RETRY_TIMES": 3, # Retry failed requests up to 3 times
    "AUTOTHROTTLE_ENABLED": False, # Disables the AutoThrottle extension (recommended to be used if you are not using proxy services)
    "RANDOMIZE_DOWNLOAD_DELAY": False, # Should not be used with proxy services. If enabled, Scrapy will wait a random amount of time (between 0.5 * DOWNLOAD_DELAY and 1.5 * DOWNLOAD_DELAY) while fetching requests from the same website
    "CONCURRENT_REQUESTS": 5, # The maximum number of concurrent (i.e. simultaneous) requests that will be performed by the Scrapy downloader
    "DOWNLOAD_TIMEOUT": 60, # Setting the timeout parameter to 60 seconds as per the ScraperAPI documentation
    "ROBOTSTXT_OBEY": False # Don't obey the Robots.txt rules
}

class CarPageSpiderTesting(scrapy.Spider):
    name = "car_page_spider" # Define the name of the spider
    custom_settings=custom_settings_dict # Define the custom settings of the spider
    

    # Send an initial request to the URL to be crawled
    def start_requests(self):
        urls = [
            "https://suchen.mobile.de/fahrzeuge/details.html?id=356111122",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=356324158",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=336623795",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=355115102",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=346480425",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=300381954",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=356442326",
        ]
        for url in urls:
            yield scrapy.Request(
                client.scrapyGet(url=url, country_code="de"),
                callback=self.parse,
                dont_filter=True,  # Don't filter duplicate requests. We are sending multiple requests to overcome the captcha
                meta={"url_to_crawl": url} # Passes the URL that we used in the scrapy.Request function to the next callback function
            )

    # Get the last page of the brand-model combo and send parallel requests to all pages from the first page until the last one 
    def parse(self, response):
        # Define a logic to retry the requests until the captcha is bypassed
        if "akamai-recaptcha" in response.text or "ak-challenge-3-8.htm" in response.text or "crypto_message-3-8.htm" in response.text:
            yield scrapy.Request(
                client.scrapyGet(url=response.meta["url_to_crawl"], country_code="de"),
                callback=self.parse,
                dont_filter=True, # Don't filter duplicate requests. We are sending multiple requests to overcome the captcha
                meta={"url_to_crawl": response.meta["url_to_crawl"]}
            )
        else:
            # inspect_response(response, self)
            open_in_browser(response)
            
        # Insert a new line to separate the results
        print("\n")
