import scrapy
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

class MobileDeSpiderParallelTesting(scrapy.Spider):
    name = "parallel_testing_spider" # Define the name of the spider
    custom_settings=custom_settings_dict # Define the custom settings of the spider
    url = "https://suchen.mobile.de/fahrzeuge/search.html?dam=0&isSearchRequest=true&ms=8600%3B2%3B%3B&ref=quickSearch&sb=rel&vc=Car" # Define the URL to be crawled

    # Send an initial request to the URL to be crawled
    def start_requests(self):
        yield scrapy.Request(
            client.scrapyGet(url=MobileDeSpiderParallelTesting.url, country_code="de"),
            callback=self.parse,
            dont_filter=True,  # Don't filter duplicate requests. We are sending multiple requests to overcome the captcha
            meta={"url_to_crawl": MobileDeSpiderParallelTesting.url} # Passes the URL that we used in the scrapy.Request function to the next callback function
        )

    # Get the last page of the brand-model combo and send parallel requests to all pages from the first page until the last one 
    def parse(self, response):
        # Define a logic to retry the requests until the captcha is bypassed
        if "akamai-recaptcha" in response.text:
            yield scrapy.Request(
                client.scrapyGet(url=response.meta["url_to_crawl"], country_code="de"),
                callback=self.parse,
                dont_filter=True, # Don't filter duplicate requests. We are sending multiple requests to overcome the captcha
                meta={"url_to_crawl": response.meta["url_to_crawl"]}
            )
        else:
            # Get the last page of the brand-model combination
            last_page = int(response.xpath("//li[@class='padding-last-button']/span/text()").get())

            # Send GET requests to all the listing pages from the first page until the last one
            for pg in range(1, last_page + 1):
                yield scrapy.Request(
                    client.scrapyGet(url=MobileDeSpiderParallelTesting.url + f"&pageNumber={pg}", country_code="de"),
                    callback=self.listing_page_parse,
                    meta={"total_num_pages": last_page, "url_to_crawl": MobileDeSpiderParallelTesting.url + f"&pageNumber={pg}"}
                )
    
    def listing_page_parse(self, response):
        # Define a logic to retry the requests until the captcha is bypassed
        if "akamai-recaptcha" in response.text:
            yield scrapy.Request(
                client.scrapyGet(url=response.meta["url_to_crawl"], country_code="de"),
                callback=self.listing_page_parse,
                dont_filter=True, # Don't filter duplicate requests. We are sending multiple requests to overcome the captcha
                meta={"total_num_pages": response.meta["total_num_pages"], "url_to_crawl": response.meta["url_to_crawl"]}
            )
        else:
            # Don't crawl the "sponsored" or the "top in category" listings 
            cars = response.xpath("//div[contains(@class, 'cBox-body cBox-body') and @class!='cBox-body cBox-body--topInCategory' and @class!='cBox-body cBox-body--topResultitem']")
            for car in cars:
                yield {
                    "titel": car.xpath(".//span[@class='h3 u-text-break-word']/text()").get(),
                    "listing_url": car.xpath("./a/@href").get(),
                    "page_num": response.xpath("//span[@class='btn btn--secondary btn--l disabled']/text()").get(),
                    "total_num_pages": response.meta["total_num_pages"]
                }
            
            # Insert a new line to separate the results
            print("\n")
