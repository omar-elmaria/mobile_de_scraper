import scrapy
from scrapy.exceptions import CloseSpider
from dotenv import load_dotenv
from twocaptcha import TwoCaptcha
from scrapy.utils.response import open_in_browser
import os
import logging
import urllib
from w3lib.html import remove_tags

# Load environment variables
load_dotenv()

# Instantiate a solver object
solver = TwoCaptcha(os.getenv("CAPTCHA_API_KEY"))
sitekey = "6Lfwdy4UAAAAAGDE3YfNHIT98j8R1BW1yIn7j8Ka"
url = "https://suchen.mobile.de/fahrzeuge/search.html?dam=0&isSearchRequest=true&ms=8600%3B51%3B%3B&ref=quickSearch&sb=rel&vc=Car"


class MobileDeCaptchaSpider(scrapy.Spider):
    name = "mobile_captcha_spider"

    def start_requests(self):
        yield scrapy.Request(
            url=url,
            callback=self.parse
        )
    
    def parse(self, response):
        try:
            result = solver.recaptcha(sitekey=sitekey, url=url)
            logging.info(f"Captcha solving succeeded. The captcha code for url {url} is: {result.get('code')}")
        except Exception as e:
            raise CloseSpider('Could not solve captcha')
        
        captcha = result.get('code')
        payload = {
            'g-recaptcha-response': captcha
            # 'c': captcha
        }
        # print(response.cookies)
        yield scrapy.Request(
            url=f"https://suchen.mobile.de/_sec/cp_challenge/verify?cpt-token={captcha}",
            # url=f"https://www.recaptcha.net/recaptcha/api2/userverify?k={sitekey}",
            body=urllib.parse.urlencode(payload), # The dictionary must be encoded to be accepted by scrapy.Request
            headers={"referer": "https://suchen.mobile.de/fahrzeuge/search.html?dam=0&isSearchRequest=true&ms=8600%3B51%3B%3B&ref=quickSearch&sb=rel&vc=Car"},
            method="GET",
            callback = self.parse_page
        )
        # open_in_browser(response)
    def parse_page(self, response):
        open_in_browser(response)
        print(response.text)
        print("\n")
        print(remove_tags(response.headers["Set-Cookie"]))
        print("\n")
        print(response.body)
        print("\n")
        print(response.flags)
        print("\n")
        print(response.request)
        print("\n")
        print(response.url)
        yield scrapy.Request(
            url=url,
            callback=self.parse_homepage,
            dont_filter=True
        )
    
    def parse_homepage(self, response):
        open_in_browser(response)