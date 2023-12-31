# Import packages
import os
from dotenv import load_dotenv
import asyncio
from zyte_api.aio.client import AsyncClient
import requests

# Load environment variables
load_dotenv()

# Define a function that extracts the product list from a URL using the Zyte API
def listing_page_crawler(url):
    api_response = requests.post(
        "https://api.zyte.com/v1/extract",
        auth=(os.getenv(key=os.getenv("ZYTE_API_KEY")), ""),
        json={
            "url": url,
            "browserHtml": True,
            "productList": True,
            "productListOptions": {"extractFrom":"browserHtml"},
        },
    )

    product_list = api_response.json()["productList"]
    return product_list

# Define a function that extracts the product list from a URL
async def main(url):
    client = AsyncClient(api_key=os.getenv("ZYTE_API_KEY"))
    api_response = await client.request_raw(
        {
            "url": url,
            "browserHtml": True,
            "productList": True,
            "productListOptions": {"extractFrom":"browserHtml"},
        }
    )
    product_list = api_response["productList"]

    return product_list

product_list = asyncio.run(main(url="https://suchen.mobile.de/fahrzeuge/search.html?isSearchRequest=true&ms=3500%3B328%3B%3B%3B&ref=dsp&s=Car&vc=Car"))
print(product_list)