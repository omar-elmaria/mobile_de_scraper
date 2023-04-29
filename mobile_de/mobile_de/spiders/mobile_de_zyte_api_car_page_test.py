import scrapy
from dotenv import load_dotenv
from scraper_api import ScraperAPIClient
import os
import re


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
    "ROBOTSTXT_OBEY": False, # Don't obey the Robots.txt rules
    "LOG_FILE": "mobile_scrapy_logs.log",
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

class CarPageSpiderTesting(scrapy.Spider):
    name = "car_page_spider" # Define the name of the spider
    custom_settings=custom_settings_dict # Define the custom settings of the spider

    # Send an initial request to the URL to be crawled
    def start_requests(self):
        urls = [
            "https://suchen.mobile.de/fahrzeuge/details.html?id=365159053&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&action=eyeCatcher&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=364545286&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=338382874&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&fnai=prev&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=365478917&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=334307354&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=364558520&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=338383179&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=338382453&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=333771155&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&action=eyeCatcher&fnai=next&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=338382060&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=362736920&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=361225703&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=363077587&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&action=eyeCatcher&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=351250774&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=361225395&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=274993479&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=360422946&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=360946606&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=363233195&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp",
            "https://suchen.mobile.de/fahrzeuge/details.html?id=365389239&damageUnrepaired=ALSO_DAMAGE_UNREPAIRED&isSearchRequest=true&makeModelVariant1.makeId=14600&makeModelVariant1.modelId=5&pageNumber=2&scopeId=C&action=eyeCatcher&searchId=e759ee16-f364-314a-df59-5f51fb9c82d5&ref=srp"
        ]
        for url in urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                dont_filter=True,  # Don't filter duplicate requests. We are sending multiple requests to overcome the captcha
                meta={"url_to_crawl": url} # Passes the URL that we used in the scrapy.Request function to the next callback function
            )

    # Get the last page of the brand-model combo and send parallel requests to all pages from the first page until the last one 
    def parse(self, response):
        # Extract the title
        titel = response.xpath("//h1[@id='ad-title']/text()").get() + " " + response.xpath("//div[@class='listing-subtitle']/text()").get()
        if titel == " ":
            titel = None

        # Extract the form
        form = response.xpath("//div[@id='category-v']/text()").get()

        # Extract the fahrzeugzustand
        fahrzeugzustand = response.xpath("//div[@id='damageCondition-v']/text()").get()

        # Extract the leistung
        leistung = response.xpath("//div[text()='Leistung']/following-sibling::div/text()").get()
        if leistung:
            leistung = float(re.findall(pattern="(?<=\()\d+", string=leistung)[0])
        
        # Extract the Getriebe
        getriebe = response.xpath("//div[text()='Getriebe']/following-sibling::div/text()").get()

        # Extract the Farbe
        farbe = response.xpath("//div[@id='color-v']/text()").get()

        # Extract the price
        preis = response.xpath("//span[@data-testid='prime-price']/text()").get()
        if preis:
            preis = int(''.join(re.findall(pattern="\d+", string=preis)))
        
        # Extract the Kilometer
        kilometer = response.xpath("//div[text()='Kilometerstand']/following-sibling::div/text()").get()
        if kilometer:
            kilometer = float(''.join(re.findall(pattern="\d+", string=kilometer)))
        
        # Extract the erstzulassung
        erstzulassung = response.xpath("//div[text()='Erstzulassung']/following-sibling::div/text()").get()

        # Extract the fahrzeughalter
        try:
            fahrzeughalter = float(response.xpath("//div[text()='Fahrzeughalter']/following-sibling::div/text()").get())
        except TypeError: # If fahrzeughalter = None, the "int" function will produce an error
            fahrzeughalter = None
        
        # Extract the standort
        standort = response.xpath("//p[@id='seller-address']/text()").get()
        if standort:
            standort = re.findall(pattern="[A-za-z]+(?=-)", string=standort)[0]

        # Extract the vehicle description and join it
        fahrzeug_beschreibung = response.xpath("//div[@class='g-col-12 description']//text()").getall()
        if fahrzeug_beschreibung is not None:
            fahrzeug_beschreibung = "\n".join(fahrzeug_beschreibung)
        else:
            fahrzeug_beschreibung = None

        yield {
            "marke": "Lamborghini",
            "modell": "Gallardo",
            "variante": "",
            "titel": titel,
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
            "fahrzeugbescheibung": fahrzeug_beschreibung,
            "url_to_crawl": response.meta["url_to_crawl"]
        }
