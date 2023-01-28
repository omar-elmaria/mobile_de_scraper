import scrapy
from dotenv import load_dotenv
import os
from scrapy.utils.response import open_in_browser

# Load environment variables
load_dotenv()

# Instantiate a solver object
url = "https://suchen.mobile.de/fahrzeuge/search.html"


class MobileZyteAPISpider(scrapy.Spider):
    name = "mobile_zyte_api_spider"
    custom_settings = {
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
    }

    def start_requests(self):
        yield scrapy.Request(
            url = url,
            meta={
                "zyte_api_automap": {
                    "browserHtml": True,
                    "actions": [
                        {
                            "action": "click",
                            "delay": 0,
                            "button": "left",
                            "onError": "return",
                            "selector": {
                                "type": "css",
                                "value": ".iBneUr.mde-consent-accept-btn.sc-bczRLJ"
                            }
                        },
                        {
                            "action": "select",
                            "values": [
                                "8600"
                            ],
                            "selector": {
                                "type": "css",
                                "value": "select#selectMake1-ds"
                            }
                        },
                        {
                            "action": "select",
                            "values": [
                                "NO_DAMAGE_UNREPAIRED"
                            ],
                            "selector": {
                                "type": "css",
                                "value": "select#damageUnrepaired-ds"
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
                        }
                    ],
                },
            },
        )
    
    def parse(self, response):
        print(response.xpath("//h1[@data-testid='result-list-headline']").get()) # Gets the title of the page