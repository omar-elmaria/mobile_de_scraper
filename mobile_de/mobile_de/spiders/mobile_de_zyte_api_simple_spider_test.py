import scrapy
from dotenv import load_dotenv
import os
import logging

# Load environment variables
load_dotenv()

class MobileZyteAPISimpleSpider(scrapy.Spider):
    name = "mobile_zyte_api_simple_spider"
    custom_settings = {
        "FEED_EXPORT_ENCODING": "utf-8", # UTF-8 deals with all types of characters
        "RETRY_TIMES": 3, # Retry failed requests up to 3 times
        "AUTOTHROTTLE_ENABLED": False, # Disables the AutoThrottle extension (recommended to be used if you are not using proxy services)
        "RANDOMIZE_DOWNLOAD_DELAY": False, # Should not be used with proxy services. If enabled, Scrapy will wait a random amount of time (between 0.5 * DOWNLOAD_DELAY and 1.5 * DOWNLOAD_DELAY) while fetching requests from the same website
        "CONCURRENT_REQUESTS": 5, # The maximum number of concurrent (i.e. simultaneous) requests that will be performed by the Scrapy downloader
        "DOWNLOAD_TIMEOUT": 60, # Setting the timeout parameter to 60 seconds as per the ScraperAPI documentation
        "ROBOTSTXT_OBEY": False, # Don't obey the Robots.txt rules
        "LOG_FILE": "mobile_scrapy_simple_spider_logs.log",
        "LOG_LEVEL": "DEBUG",
        
        # Zyte API settings
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
            "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        },
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000,
        },
        "REQUEST_FINGERPRINTER_CLASS": "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "ZYTE_API_KEY": os.getenv("ZYTE_API_KEY"),
        "ZYTE_API_LOG_REQUESTS": True,
        "ZYTE_API_SKIP_HEADERS": ["Cookie", "User-Agent"],
        "ZYTE_API_RETRY_POLICY": "retry_policies.CUSTOM_RETRY_POLICY"
    }
    # Instantiate a solver object
    base_url = "https://suchen.mobile.de/fahrzeuge/search.html"

    def start_requests(self):
        yield scrapy.Request(
            url=MobileZyteAPISimpleSpider.base_url,
            callback=self.parse,
            meta={
                "zyte_api_automap": {
                    "browserHtml": True,
                    "javascript": True,
                    # Actions are as follows:
                    # 1- Wait for the Accept Cookies selector to appear 
                    # 2- Click on the Accept Cookies button 
                    # 3- Select the car brand (14600 is Lamborghini) 
                    # 4- Select the car model (5 is Gallardo) 
                    # 5- Select the car model (5 is Gallardo) 
                    # 6- Click on the search button 
                    # 7- Wait for the headline of the results page to appear . It should say "111 Angebote entsprechen Deinen Suchkriterien"
                    # 8- Wait for the car listings to appear. We should have 20 listings per page
                    "actions": [
                        {
                            "action": "waitForSelector",
                            "onError": "return",
                            "selector": {
                                "type": "css",
                                "value": ".iBneUr.mde-consent-accept-btn.sc-bczRLJ",
                                "state": "attached"
                            },
                            "timeout": 15
                        },
                        {
                            "action": "click",
                            "delay": 0,
                            "button": "left",
                            "onError": "return",
                            "selector": {
                                "type": "css",
                                "value": ".iBneUr.mde-consent-accept-btn.sc-bczRLJ",
                                "state": "attached"
                            }
                        },
                        {
                            "action": "select",
                            "onError": "return",
                            "values": [
                                "14600"
                            ],
                            "selector": {
                                "type": "xpath",
                                "value": "//select[@name='makeModelVariant1.makeId']",
                                "state": "attached"
                            }
                        },
                        {
                            "action": "waitForTimeout",
                            "timeout": 5,
                            "onError": "return"
                        },
                        {
                            "action": "select",
                            "onError": "return",
                            "values": [
                                "5" # Gallardo 5
                            ],
                            "selector": {
                                "type": "xpath",
                                "value": "//select[@name='makeModelVariant1.modelId']",
                                "state": "attached"
                            }
                        },
                        {
                            "action": "click",
                            "delay": 0,
                            "button": "left",
                            "onError": "return",
                            "selector": {
                                "type": "css",
                                "value": "button#dsp-upper-search-btn > span > span"
                            }
                        },
                        {
                            "action": "waitForSelector",
                            "onError": "return",
                            "selector": {
                                "type": "xpath",
                                "value": "//h1[@data-testid='result-list-headline']",
                                "state": "attached"
                            },
                            "timeout": 15
                        },
                        {
                            "action": "waitForSelector",
                            "onError": "return",
                            "selector": {
                                "type": "xpath",
                                "value": "//div[contains(@class, 'cBox-body cBox-body') and @class!='cBox-body cBox-body--topInCategory' and @class!='cBox-body cBox-body--topResultitem']",
                                "state": "attached"
                            },
                            "timeout": 15
                        }
                    ],
                },
            },
        )
    
    def parse(self, response):
        # Print out the list of actions done by the Zyte API
        logging.info(f"The list of actions done by the Zyte API for the Mobile website's search page is: {response.raw_api_response['actions']}")
        
        # Obtain the heading of the page. It should be --> 112 Angebote entsprechen Deinen Suchkriterien
        logging.info(response.xpath("//h1[@data-testid='result-list-headline']").get()) # Gets the title of the page

        # Get the last page of the brand-model combination
        last_page_list = response.xpath("//span[@class='btn btn--secondary btn--l']/text()").getall()
        try:
            last_page = int(last_page_list[-1])
        except IndexError: # The index error can occur if the brand has only one page. In that case, set last_page to 1
            last_page = 1
        logging.info(f"We have a total of {last_page} pages to loop through...")

        # Send GET requests to all the listing pages from the first page until the last one. These requests will give us the URLs of the individual cars
        for pg in range(1, last_page + 1):
            logging.info(f"Sending a request to page number {pg}\n")
            yield scrapy.Request(
                url=response.url + f"&pageNumber={pg}",
                callback=self.listing_page_parse,
                meta={
                    "total_num_pages": last_page,
                    "page_to_crawl": pg,
                    "url_to_crawl": response.url + f"&pageNumber={pg}"
                },
                dont_filter=True
            )
    
    # Retrieve the URLs of the individual cars from ALL listing pages using the requests that were sent from the "parse" function
    def listing_page_parse(self, response):
        # Don't crawl the "sponsored" or the "top in category" listings 
        cars = response.xpath("//div[contains(@class, 'cBox-body cBox-body') and @class!='cBox-body cBox-body--topInCategory' and @class!='cBox-body cBox-body--topResultitem']")
        logging.info(f"The number of cars on page {response.meta['page_to_crawl']} is: {len(cars)}")
        for car in cars:
            # Get the URL of the car and send a GET request to retrieve the data on it
            car_url = car.xpath("./a/@href").get()
            logging.info(f"Extracting the car data from this individual car page {car_url}")
            yield scrapy.Request(
                url=car_url,
                callback=self.car_page_parse,
                meta={
                    "total_num_pages": response.meta["total_num_pages"],
                    "page_rank": int(response.xpath("//span[@class='btn btn--secondary btn--l disabled']/text()").get()),
                    "url_to_crawl": car_url
                }
            )
    
    # Crawl data from the individual car page using the requests that were sent from the "listing_page_parse" function
    def car_page_parse(self, response):
        # Extract the title
        titel = response.xpath("//h1[@id='ad-title']/text()").get() + " " + response.xpath("//div[@class='listing-subtitle']/text()").get()
        if titel == " ":
            titel = None

        yield {
            "marke": "Lamborghini",
            "modell": "Gallardo",
            "titel": titel,
            "url_to_crawl": response.meta["url_to_crawl"],
            "page_rank": response.meta["page_rank"],
            "total_num_pages": response.meta["total_num_pages"],
        }
        
    # Insert a new line to separate the results
    logging.info("\n")