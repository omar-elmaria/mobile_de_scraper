# Import packages
import asyncio
import json
import os
import re

import pandas as pd
import requests
from dotenv import load_dotenv
from zyte_api.aio.client import AsyncClient

# Load environment variables
load_dotenv()

# Define a function that extracts the car page URL list from a URL using the Zyte API and requests
def listing_page_crawler_requests(url):
    print(f"Extracting car page list from {url}...")
    api_response = requests.post(
        "https://api.zyte.com/v1/extract",
        auth=(os.getenv("ZYTE_API_KEY"), ""),
        json={
            "url": url,
            "browserHtml": True,
            "productList": True,
            "productListOptions": {"extractFrom":"browserHtml"},
        },
    )

    car_page_list = api_response.json()["productList"]
    return car_page_list

# Define a function that extracts the car page URL list from a URL using the Zyte API and AsyncClient
async def listing_page_crawler_client(url):
    print(f"Extracting car page list from {url}...")
    client = AsyncClient(api_key=os.getenv("ZYTE_API_KEY"))
    api_response = await client.request_raw(
        {
            "url": url,
            "browserHtml": True,
            "productList": True,
            "productListOptions": {"extractFrom":"browserHtml"},
        }
    )
    car_page_list = api_response["productList"]

    return car_page_list

# Define a function that extracts the number of listings from the car page list JSON
def num_listings_extractor(json_data):
    # Extract the number of listings in raw format (181 ANgebote)
    num_listings_raw = [i["name"] for i in json_data["breadcrumbs"] if "Angebote" in i["name"]][0]
    num_listings = re.findall(pattern=r"\d+", string=num_listings_raw)[0]

    return num_listings

# Define a function that extracts the car page URLs from the car page list JSON
def car_page_url_extractor(json_data):
    # Extract the number of listings in raw format (181 ANgebote)
    car_page_url_list = [i["url"] for i in json_data["products"]]

    return car_page_url_list

###------------------------------------###------------------------------------###

# Open the JSON file that contains the target URLs
with open("target_url_list_cat_all.json", "r") as f:
    df_target_url = pd.DataFrame(json.load(f), columns=["marke", "modell", "listing_page_url"])
    f.close()

# Extract the car page list from the target URLs. First, we extract the number of "Angebote" and then send requests to all the pages of the marke-modell combo
df_target_url["num_listings"] = df_target_url["listing_page_url"].apply(lambda x: num_listings_extractor(listing_page_crawler_requests(x)))
print(df_target_url)

# car_page_list = asyncio.run(listing_page_crawler_client(
#     url="https://suchen.mobile.de/fahrzeuge/search.html?isSearchRequest=true&ms=3500%3B328%3B%3B%3B&ref=dsp&s=Car&vc=Car")
# )