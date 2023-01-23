import scrapy
from dotenv import load_dotenv
from scraper_api import ScraperAPIClient
import os
from scrapy.utils.response import open_in_browser
from scrapy.shell import inspect_response


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
    "CONCURRENT_REQUESTS": 50, # The maximum number of concurrent (i.e. simultaneous) requests that will be performed by the Scrapy downloader
    "DOWNLOAD_TIMEOUT": 60, # Setting the timeout parameter to 60 seconds as per the ScraperAPI documentation
    "ROBOTSTXT_OBEY": False # Don't obey the Robots.txt rules
}

headers_string = '''
    accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9
    accept-encoding: gzip, deflate, br
    accept-language: en-GB,en-US;q=0.9,en;q=0.8,de;q=0.7
    cache-control: max-age=0
    cookie: optimizelyEndUserId=oeu1670587582758r0.8917445448030299; vi=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjaWQiOiJhYjIxZjVlYy04NTkxLTQ0NjgtYTM1NC00YzI2N2JhNzJiNTIiLCJpYXQiOjE2NzA1ODc2NjV9.sDTIpu9PX7fgMRURKL_-pbU6NaVtxVSECsugdkgFhgY; mdeConsentDataGoogle=1; mdeConsentData=CPjv3KvPjv3KvEyAHADECtCgAP_AAELAAAYgJLNd_H__bX9v-f7_6ft0eY1f9_r37uQzDhfNs-8F3L_W_LwX_2E7NF36tq4KmR4ku1LBIUNtHMnUDUmxaokVrzHsak2cpzNKJ_BkknsZe2dYGF9vm5tj-QKZ7_5_d3f52T_9_9v-39z33913v3d93-_13Pjd_5_9H_v_fR_b8_Kf9_7-_4v8_____3_e______9___AvOAOAAoAEAANAAigBMAC2AvMAkJAQAAWABUADIAHAARAAyAB4AEQAJ4AVQBhgD9ASIAyQBk4DLg0AUAJgAXADqgJEAZOIgCABMAOqAkQBk4qAGAEwALgC8xkAIAJgC8x0BUABYAFQAMgAcABEADIAHgAPgAiABPACqAFwAMQAmABhgD9AIsAkQBkgDJwGXEIBIACwAMgAiACYAFUALgAYgEiAMnJQCwAFgAZAA4ACIAHgARAAqgBcADEAkQBk5SAkAAsACoAGQAOAAiABkADwAIgATwApABVADEAP0AiwCRAGSAMnAZcA.YAAAAAAAD4AAAKcAAAAA; _gcl_au=1.1.1268603295.1670587665; __gads=ID=0e769807f99add85:T=1670587665:S=ALNI_Mb2YU78fN80r4teFqn6B2LwXAXsCA; _pbjs_userid_consent_data=4012353048095475; _pubcid=ebf8f133-788b-42c5-a369-95b1ef094594; axd=4305844686403245439; _fbp=fb.1.1670587795326.164167016; __gsas=ID=7ecdec070cbe10d4:T=1670587797:S=ALNI_MagMZAQeO-vG5NFzfk4RNanK-inOQ; _tt_enable_cookie=1; _ttp=ulmtFBML6JGBdC7hdq220bZa6Bb; kk-vip-80-data=%7B%22id%22%3A%22346480425%22%2C%22title%22%3A%22Ferrari%20458%20V8%20Italia%20Capristo%20Racing%20Carbon%20Sitze%22%2C%22price%22%3A%22173.999%C2%A0%E2%82%AC%22%2C%22image%22%3A%22img.classistatic.de%2Fapi%2Fv1%2Fmo-prod%2Fimages%2F43%2F436778d7-3d96-4ce3-ba3e-089787f65680%22%2C%22mileage%22%3A%2245.800%C2%A0km%22%2C%22fuel%22%3A%22Benzin%22%2C%22power%22%3A%22419%C2%A0kW%C2%A0(570%C2%A0PS)%22%2C%22firstRegistration%22%3A%2204%2F2014%22%2C%22phoneNumber%22%3A%22%2B49%20(0)811%201244998754%22%7D; mdeTheme=system; tis=EP117%3A3406; _gid=GA1.2.1225335537.1674473464; __gpi=UID=00000b8fb2c380e5:T=1670587665:RT=1674473461:S=ALNI_MZ4blSsFE-Su1apLU7v3sndvGP7XQ; iom_consent=0103ff03ff&1674473463923; _uetsid=6be443e09b1111ed9ca31f058c8d5a57; _uetvid=736784b077ba11eda8ba2f5ac7101a32; cto_bundle=BuWSSl9nTlR0eGxEMEw3WFRiWnROQ1JQeHRzRW9ndlVRendrTmtxMkZlM0dWSHIzS0xXeTNDOUZ6VW1DV1VOTUJnWGElMkZSSXZNZWFEJTJGTk12S1VPVldsRjJ1MHM4R0QyMDVVdVVZOHZNd3MzUHB0MTVwSlA3dmxoS1dyWnpMNWZOeDRYdW5MZ2hoWDJzb0tmMWZ6JTJCOWc3VzBiUEElM0QlM0Q; ak_bmsc=0DF1523799F8CBD606FEC61956318E16~000000000000000000000000000000~YAAQXfAWAmCHAdCFAQAA/0Qn3xK2NymbNZQgzn+haegKSA6RZlkXcuwFEapI1WLoNAMqYeqBjVejAToHOzeXvokrHwjPwwXtDTEGOsPzIC2P87wXxD6g+fbhtO7BrpgkvJc8GlmwUA0yeW5V8t9/FA4EJjlG1An0FmdhE/GTrrT5Yk3b9aeehoEkbkRokz0kel+1L/0aUifS0mPfUHVVJbHDMoi94qVe7ErTPZy2DAMnzJDmnLXUFszck9yejEVa9lkuCXxZ2dle6V6NZiHPw/6e21WfUD23z+nY9f8q/RBzzEwl4H6cfnYUartM/qQhUkpBtW7ZzdngZ+TL4+grbmihQceY54ktk3JRpsZCRvxKCyNfAWtvjPKG1Qr4AshI7CVEnD3gaXp9jvUhy+eH1GQk0c2pzdJDwYoqhog+Xhpq5Rf6Xp8k8bxkytVVsJ4vmP/gBcoNZWjm007xHEEP7Ut2ZEZPa9a1JOzE7YLqJDqOgIGzbe/I; mobile.LOCALE=de; bm_sz=F4668EF1B530CBF886A8A3CF51F359E9~YAAQFIQUAvmrEt+FAQAA3uqM3xKubX3my7DESTJ/pKgaTjXJXeYIdGEjpwZMIWqDVZYjqjeaCskztoux8U7aExKU4vNYrFTtFWy63/6mQHLdpjaVSiK8ZxDyLotzUAlmqHj9iyR21K6pS1cvFDtWfFMFD7GyJCZWsheatcbI94vSABjGoLrienbZalfWO8s74rBSVR1FKpwxSQoRSrMy6GrNcANHxlds0rxQzmRb1tTruPmwdYUtfKWARyi/Mb827/wFRnxPpwZCvjoXocu4s2nkGZUGOlEo262qRhKUx17FGw==~4538937~3752246; ces=-; _ga_2H40T2VTNP=GS1.1.1674492835.20.1.1674492835.0.0.0; _ga=GA1.2.1174753961.1670587585; lux_uid=167449283533179785; ioam2018=000c37aec80cde56b63932512:1697458066202:1670587666202:.mobile.de:106:mobile:DE/DE/OB/S/P/S/G:noevent:1674492835607:xdvhpy; POPUPCHECK=1674579235608; _abck=586806B81CFD2841A067AEDF40E59AA4~-1~YAAQFIQUAnngEt+FAQAAMGCO3wnSYYTPFCBPI0lIdLhk7dI5ITP0H/EYK6cXtAtlvF64UwSO2mHwtalZ4RuiYogNqnLcwXvIdlxkzqJdzgfh9TDG0uuZ6LmFZGbqnHgmRZZX7Ive3xujJ5jGBdqPjcRrqDUUaXw7dgwtTVnH6OvK9W0TnAa546w/WXsXBgcmLMu6u6UW1FuOFegcjDfYlu1LuqnTlNHPpvOZ+2XXQaq6nvh8YI3R286zPhugRSrJSXGqIIyTlBhDLDd+TICL6s4vX0FLUp0dgIL7GUJQpSIt0CfMcSX4BAHy+QWQNTbLALQvD349YNjb3DirwEOXJtK8nwW5mfwfSZvHKnF0Spe5bHM0sWMgV0qFeqK8g+fwXZex17xoekjfCUkFRY0WmKFT9M+BqSA=~0~-1~-1; bm_sv=8BDC91B2FAD6D2330ED22256520CC280~YAAQFIQUAnrgEt+FAQAAMGCO3xI8MFZuUhWor0JvJW9HPwOrXo4MPlIuhlbG9q2kv/M5LUbzEtm7r7w+XLFvAMxquu4jMePT3VFLkaRzRQ1mT8fKUAiFjmgYDLPWf7Qn9B45sAZmfMkHPf7BchBrwd3HX10Dhr/vajh49GGYJEjMFiqRASsfUpJ71Burlg6h8xlfUipx6LEYWLha4ut2/Fmm/oOYxIf/i0lhBzNEzW+Sd1v2aTvkjnxIzzra9yju~1; sec_cpt=5D49C70AD674C5F5350BAB6A5B777593~3~YAAQFIQUAnTuEt+FAQAA/MCO3wgKaMmjP+EvxkQ+Z85XLnIwh+ZH7wDBf7rp8TjSDnvCh+ijmNnM0ftTgIQ8kpSPO3PdTkIB0zkiyR4fzoui+TkiO2DlT38xCl434RKCxUSyE5EYk59tjcdBl2oggCEocmn5tSPvEsdJC2EHo0WexMSWlQU56hHL3edGcCeiwvw8tXwhd+Mm2zRZCziUN6zJ5JdJpZxfWbbTq7SCDKtXUqCVRy/WhyQ0lgU0mT2RE0aPze0XFJqcdUu6ZBmQhHReEu4Aw12gEgOaESzEL1K/MNGyyByKKz9+9//MrLnrDqHMkkcpCokToVVwlWVRQYsM9OVS65lWq8M7lcXzng5D+CJziBcasr5cPp1K3CHOMgVtxBM3tHQglH88YRIZ6SFApPagSGN7Dz4f8ggWpZdm3/Vuc19VbKPh3EmUgI2V997aj8CxiqoJKdkp0verMCnKH1aKo5YWTLH9+1wLiXCVwjdp+T7EFlcE+vwwkQ4jKC8ASlDsaXSKwE8o69M7sfiyCwMpjylF
    referer: https://suchen.mobile.de/fahrzeuge/search.html?dam=0&isSearchRequest=true&ms=8600%3B51%3B%3B&ref=quickSearch&sb=rel&vc=Car
    sec-ch-ua: "Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"
    sec-ch-ua-mobile: ?0
    sec-ch-ua-platform: "Windows"
    sec-fetch-dest: document
    sec-fetch-mode: navigate
    sec-fetch-site: same-origin
    sec-fetch-user: ?1
    upgrade-insecure-requests: 1
    user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36
'''

# Define a function to convert headers copied from the browser to a dictionary
def get_headers(s, sep=': ', strip_cookie=False, strip_cl=False, strip_headers: list = []) -> dict():
    d = dict()
    for kv in s.split('\n'):
        kv = kv.strip()
        if kv and sep in kv:
            v=''
            k = kv.split(sep)[0]
            if len(kv.split(sep)) == 1:
                v = ''
            else:
                v = kv.split(sep)[1]
            if v == '\'\'':
                v =''
            if strip_cookie and k.lower() == 'cookie': continue
            if strip_cl and k.lower() == 'content-length': continue
            if k in strip_headers: continue
            d[k] = v
    return d

class TestSpider(scrapy.Spider):
    name = "test_spider" # Define the name of the spider
    custom_settings=custom_settings_dict # Define the custom settings of the spider
    url = "https://suchen.mobile.de/fahrzeuge/search.html?dam=0&isSearchRequest=true&ms=8600%3B51%3B%3B&ref=quickSearch&sb=rel&vc=Car" # Define the URL to be crawled

    # Send an initial request to the URL to be crawled
    def start_requests(self):
        yield scrapy.Request(
            url=TestSpider.url,
            callback=self.parse,
            dont_filter=True,  # Don't filter duplicate requests. We are sending multiple requests to overcome the captcha
            meta={"url_to_crawl": TestSpider.url}, # Passes the URL that we used in the scrapy.Request function to the next callback function
            headers=get_headers(headers_string)
        )
    
    def parse(self, response):
        open_in_browser(response)