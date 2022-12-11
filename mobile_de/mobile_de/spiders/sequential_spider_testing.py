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

class MobileDeSpiderSequentialTesting(scrapy.Spider):
    name = "sequential_testing_spider"
    custom_settings=custom_settings_dict
    url = "https://suchen.mobile.de/fahrzeuge/search.html?dam=0&isSearchRequest=true&ms=8600%3B2%3B%3B&ref=quickSearch&sb=rel&vc=Car"

    def start_requests(self):
        # Don't filter duplicate requests. We are sending multiple requests to overcome the captcha
        yield scrapy.Request(
            client.scrapyGet(url=MobileDeSpiderSequentialTesting.url, country_code="de"),
            callback=self.parse,
            dont_filter=True,
            meta={"url_to_crawl": MobileDeSpiderSequentialTesting.url}
        )

    def parse(self, response):
        if "akamai-recaptcha" in response.text:
            # Don't filter duplicate requests. We are sending multiple requests to overcome the captcha
            yield scrapy.Request(client.scrapyGet(url=response.meta["url_to_crawl"], country_code="de"), callback=self.parse, dont_filter=True, meta={"url_to_crawl": response.meta["url_to_crawl"]})
        else:
            # Don't crawl the "sponsored" or the "top in category" listings 
            cars = response.xpath("//div[contains(@class, 'cBox-body cBox-body') and @class!='cBox-body cBox-body--topInCategory' and @class!='cBox-body cBox-body--topResultitem']")
            for car in cars:
                yield {
                    "titel": car.xpath(".//span[@class='h3 u-text-break-word']/text()").get(),
                    "listing_url": car.xpath("./a/@href").get(),
                    "page_num": response.xpath("//span[@class='btn btn--secondary btn--l disabled']/text()").get()
                }
            
            # Insert a new line to separate the results
            print("\n")

            # Navigate to the next page(s)
            next_page = response.xpath("//li[@class='pref-next u-valign-sub']/span[@id='page-forward']/@data-href").get()
            if next_page is not None:
                yield scrapy.Request(
                    client.scrapyGet(url=next_page, country_code="de"),
                    callback=self.parse,
                    meta={"url_to_crawl": next_page}
                )
