import scrapy
from scrapy.exceptions import CloseSpider
from dotenv import load_dotenv
from twocaptcha import TwoCaptcha
from scrapy.utils.response import open_in_browser
import os
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from shutil import which
from scrapy_selenium import SeleniumRequest

# Load environment variables
load_dotenv()

# Instantiate a solver object
solver = TwoCaptcha(os.getenv("CAPTCHA_API_KEY"))
sitekey = "6Lfwdy4UAAAAAGDE3YfNHIT98j8R1BW1yIn7j8Ka"
url = "https://suchen.mobile.de/fahrzeuge/search.html?dam=0&isSearchRequest=true&ms=8600%3B51%3B%3B&ref=quickSearch&sb=rel&vc=Car"


class MobileDeCaptchaSpider(scrapy.Spider):
    name = "mobile_captcha_spider"
    custom_settings = {
        "SELENIUM_DRIVER_NAME": 'chromium',
        "SELENIUM_DRIVER_EXECUTABLE_PATH": which('chromedriver'),
        "SELENIUM_DRIVER_ARGUMENTS": ['--headless'],
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy_selenium.SeleniumMiddleware': 800
        }
    }

    def start_requests(self):
        try:
            result = solver.recaptcha(sitekey=sitekey, url=url)
            logging.info(f"Captcha solving succeeded. The captcha code for url {url} is: {result.get('code')}")
        except Exception:
            raise CloseSpider('Could not solve captcha')
        
        captcha_key = result.get('code')

        yield SeleniumRequest(
            url=f"https://suchen.mobile.de/fahrzeuge/search.html?dam=0&isSearchRequest=true&ms=8600%3B51%3B%3B&ref=quickSearch&sb=rel&vc=Car",
            wait_until=EC.frame_to_be_available_and_switch_to_it((By.ID, "sec-cpt-if")),
            callback=self.parse_page,
            script=f"document.getElementById('g-recaptcha-response').innerHTML='{captcha_key}'; verifyAkReCaptcha('{captcha_key}');",
            headers={
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
                "cache-control": "max-age=0",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "accept-encoding": "gzip, deflate, br"
            }
        )
    
    def parse_page(self, response):
        open_in_browser(response)
        print(response.xpath("//h1[@data-testid='result-list-headline']").get())