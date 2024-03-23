import json
import logging
import os
import re
from datetime import datetime, timedelta

import pandas as pd
import pytz
import yagmail
from google.cloud import bigquery
from google.oauth2 import service_account

from gdrive_upload_script import upload_file_to_gdrive
from inputs import (
    change_cwd,
    listing_page_crawling_framework,
    marke_list,
    modell_list
)
from mobile_de.spiders.mobile_de_zyte_api_car_page_spider import run_car_page_spider
from mobile_de_selenium_code_prod_listing_page_func import (
    date_start_for_log_file_name,
    mobile_de_local_single_func
)
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from twisted.internet import defer, reactor

def is_between_time_range():
    # Set the timezone to CET
    cet_timezone = pytz.timezone('CET')

    # Get the current time in CET
    current_time_cet = datetime.now(cet_timezone)

    # Check if it's Friday and the time is between 11:00 pm and 11:05 pm
    if current_time_cet.weekday() == 4:  # 4 corresponds to Friday
        start_time = current_time_cet.replace(hour=23, minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(minutes=5)
        return start_time <= current_time_cet <= end_time
    else:
        return False

def main():
    # Mark the start of the script
    t1 = datetime.now()

    # Set the G-drive folder ID. The Folder ID is the ID of the folder called "vm_logs"
    gdrive_folder_id="16e4f41zhwV67Pm01I0jHwL2l59kpn8WY"

    # Run the Selenium script that crawls the listing page
    if listing_page_crawling_framework == "selenium":
        # Print a status message indicating the start of the selenium script
        logging.info("Running the selenium crawler to get the listing page URLs...")
        
        # Run the selenium script
        mobile_de_local_single_func(
            category="cat_all",
            car_list=marke_list,
            modell_list=modell_list,
            captcha_solver_default="capmonster"
        )

        # Print a status message indicating the end of the selenium script
        logging.info("The Selenium script finished running. Now, running the car page Scrapy spider...")

        # Run the car page spider
        run_car_page_spider()
    elif listing_page_crawling_framework == "zyte":
        # Configure logging
        configure_logging()

        # Define two Crawler runners, one for each spider
        runner_listing_page = CrawlerRunner(settings={
            "FEEDS": {"car_page_url_list_cat_all.json":{"format": "json", "overwrite": True, "encoding": "utf-8"}}, # Set the name of the output JSON file
            "LOG_FILE": f"mobile_logs_cat_all_{date_start_for_log_file_name}.log" # Set the name of the log file
        })
        runner_car_page = CrawlerRunner(settings={
            "FEEDS": {"df_all_brands_data_cat_all.json":{"format": "json", "overwrite": True, "encoding": "utf-8"}}, # Set the name of the output JSON file
            "LOG_FILE": f"mobile_logs_cat_all_{date_start_for_log_file_name}.log" # Set the name of the log file
        })

        @defer.inlineCallbacks
        def crawl():
            # Print a status message indicating the start of the zyte spider to crawl the listing page URLs
            logging.info("Running the Scrapy spider to get the listing page URLs...")
            # Run the first crawler that crawls the listing page URLs
            from mobile_de.spiders.mobile_de_zyte_api_listing_page_spider import ListingPageSpider
            yield runner_listing_page.crawl(ListingPageSpider)

            # Print a status message indicating the start of the zyte spider to crawl the car page URLs
            logging.info("The listing page Scrapy spider finished running. Now, running the car page Scrapy spider...")
            # Run the second crawler that crawls the car pages themselves
            from mobile_de.spiders.mobile_de_zyte_api_car_page_spider import CarPageSpider
            yield runner_car_page.crawl(CarPageSpider)

            # Stop the reactor
            reactor.stop()

        # Run the crawl() function
        crawl()
        reactor.run() # the script will block here until the last crawl call is finished

    # Print a status message
    logging.info("The Scrapy spider that crawls the car pages finished running. Now, cleaning the data...")

    # Retrieve the output data from the JSON file
    with open("df_all_brands_data_cat_all.json", mode="r", encoding='utf-8') as f:
        df_data_all_car_brands = json.load(f)
        df_data_all_car_brands = pd.DataFrame(df_data_all_car_brands)
        f.close()

    # Drop the duplicates because the mileage filters applied above could have produced duplicates
    df_data_all_car_brands = df_data_all_car_brands.drop_duplicates(["url_to_crawl"])

    # Clean the data
    df_data_all_car_brands_cleaned = df_data_all_car_brands.copy()
    df_data_all_car_brands_cleaned.replace(to_replace="", value=None, inplace=True)
    df_data_all_car_brands_cleaned["leistung"] = df_data_all_car_brands_cleaned["leistung"].apply(lambda x: int(re.findall(pattern=r"(?<=\().*(?=\sPS)", string=x)[0].replace(".", "")) if x is not None else x)
    df_data_all_car_brands_cleaned["preis"] = df_data_all_car_brands_cleaned["preis"].apply(lambda x: int(''.join(re.findall(pattern=r"\d+", string=x))) if x is not None else x)
    df_data_all_car_brands_cleaned["kilometer"] = df_data_all_car_brands_cleaned["kilometer"].apply(lambda x: int(''.join(re.findall(pattern=r"\d+", string=x))) if x is not None else x)
    df_data_all_car_brands_cleaned["fahrzeughalter"] = df_data_all_car_brands_cleaned["fahrzeughalter"].apply(lambda x: int(x) if x is not None else x)
    df_data_all_car_brands_cleaned["standort"] = df_data_all_car_brands_cleaned["standort"].apply(lambda x: re.findall(pattern=r"[A-za-z]+(?=-)", string=x)[0] if x is not None else x)
    df_data_all_car_brands_cleaned["crawled_timestamp"] = datetime.now()

    # Print a status message
    logging.info("Cleaning the data is done. Now, uploading the data to BigQuery...")

    # Upload to bigquery
    # First, set the credentials
    key_path_home_dir = os.path.expanduser("~") + "/bq_credentials.json"
    credentials = service_account.Credentials.from_service_account_file(
        key_path_home_dir, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    # Now, instantiate the client and upload the table to BigQuery
    client = bigquery.Client(project="web-scraping-371310", credentials=credentials)
    job_config = bigquery.LoadJobConfig(
        schema = [
            bigquery.SchemaField("marke", "STRING"),
            bigquery.SchemaField("modell", "STRING"),
            bigquery.SchemaField("variante", "STRING"),
            bigquery.SchemaField("titel", "STRING"),
            bigquery.SchemaField("form", "STRING"),
            bigquery.SchemaField("fahrzeugzustand", "STRING"),
            bigquery.SchemaField("leistung", "FLOAT64"),
            bigquery.SchemaField("getriebe", "STRING"),
            bigquery.SchemaField("farbe", "STRING"),
            bigquery.SchemaField("preis", "INT64"),
            bigquery.SchemaField("kilometer", "FLOAT64"),
            bigquery.SchemaField("erstzulassung", "STRING"),
            bigquery.SchemaField("fahrzeughalter", "FLOAT64"),
            bigquery.SchemaField("standort", "STRING"),
            bigquery.SchemaField("fahrzeugbeschreibung", "STRING"),
            bigquery.SchemaField("url_to_crawl", "STRING"),
            bigquery.SchemaField("page_rank", "INT64"),
            bigquery.SchemaField("total_num_pages", "INT64"),
            bigquery.SchemaField("crawled_timestamp", "TIMESTAMP"),
        ]
    )
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    # Upload the table
    client.load_table_from_dataframe(
        dataframe=df_data_all_car_brands_cleaned,
        destination="web-scraping-371310.crawled_datasets.lukas_mobile_de",
        job_config=job_config
    ).result()

    # Print a status message
    logging.info("Uploading the data to BigQuery is done. Now, uploading the logs to G-drive...")

    # Upload the logs to G-drive
    upload_file_to_gdrive(filename=f"mobile_logs_cat_all_{date_start_for_log_file_name}.log", folder_id=gdrive_folder_id)

    # Print a status message
    logging.info("Uploading the logs to G-drive is done. Now, sending an success E-mail...")
    
    # Send success E-mail
    yag = yagmail.SMTP("omarmoataz6@gmail.com", oauth2_file=os.path.expanduser("~")+"/email_authentication.json", smtp_ssl=False)
    contents = [
        f"This is an automated notification to inform you that the mobile.de scraper ran successfully.\nThe crawled brands are {df_data_all_car_brands_cleaned['marke'].unique()}"
    ]
    yag.send(["omarmoataz6@gmail.com", "stukenborg.lukas@gmx.de"], f"The Mobile.de Scraper Ran Successfully on {datetime.now()} CET", contents)

    # logging.info a status message marking the end of the script
    t2 = datetime.now()
    logging.info(f"The script finished at {t2}. It took {t2-t1} to crawl all listings...")    

if __name__ == '__main__':
    # Change the current working directory if needed
    change_cwd()

    # Run the script
    main()
