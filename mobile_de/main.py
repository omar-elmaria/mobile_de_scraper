from scrapy.crawler import CrawlerProcess
from mobile_de.spiders.mobile_de_spider import MobileDeSpider
import gspread
import json
import datetime as dt
import pandas as pd


def main():
    # Run the spider
    process = CrawlerProcess()
    process.crawl(MobileDeSpider)
    process.start()

    # Retrieve the output data from the JSON file
    with open("output.json", mode="r", encoding='utf-8') as f:
        data = json.load(f)
        data = pd.DataFrame(data)
        f.close()

    # Define a function that exports the data to this Google Sheet
    sa = gspread.service_account() # Invoke the service account credentials
    sh = sa.open("lukas_demo_output_ferrari_458") # Instantiate a Spreadsheet object
    wks = sh.add_worksheet(title=f"Output_{dt.datetime.now()}", rows=len(data), cols=len(data.columns)) # Add a new worksheet with a timestamp
    wks.update([data.columns.values.tolist()] + data.values.tolist()) # Publish the results to the G-sheet

if __name__ == '__main__':
    main()