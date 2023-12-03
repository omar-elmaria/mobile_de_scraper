import json
import logging
import os
import re
import time
from datetime import datetime, timedelta

import pandas as pd
import pytz
import yagmail
from gdrive_upload_script import upload_file_to_gdrive
from google.cloud import bigquery
from google.oauth2 import service_account
from mobile_de_selenium_code_prod_listing_page_func import (
    date_start_for_log_file_name,
    mobile_de_local_single_func
)
from scrapy.crawler import CrawlerProcess

from mobile_de.spiders.mobile_de_zyte_api_car_page_spider import CarPageSpider

crawl_now = False
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
    mobile_de_local_single_func(
        category="cat_all",
        car_list=[
            # Cat 1
            "ALPINA",
            "Aston Martin",
            "Bentley",
            "Bugatti",
            "Ferrari",
            "Koenigsegg",
            "KTM",
            "Lamborghini",
            "McLaren",
            "Pagani",
            "Rolls-Royce",
            "Wiesmann",

            # Cat 2
            "Audi",

            # Cat 3
            "Mercedes-Benz",

            # Cat 4
            "BMW",

            # Cat 5
            "Corvette",
            "Dodge",
            "Nissan",
            "Ford",
            "Alfa Romeo",
            "Jaguar",
            "Lexus",
            "Lotus",
            "Maserati",
            "Honda",

            # Cat 6
            "Porsche"
        ],
        modell_list=[
            # Cat 1
            # ALPINA
            "Andere",

            # Aston Martin
            "DB",
            "DB11",
            "DB9",
            "DBS",
            "DBX",
            "Rapide",
            "V12 Vantage",
            "V8 Vantage",
            "Vanquish",
            "Virage",
            "Andere",

            # Bentley
            "Bentayga",
            "    Continental",
            "    Continental Flying Spur",
            "    Continental GT",
            "    Continental GTC",
            "    Continental Supersports",
            "Flying Spur",
            "Andere",

            # Bugatti
            "Chiron",
            "EB 110",
            "Veyron",
            "Andere",

            # Ferrari
            "296 GTB",
            "360",
            "365",
            "458",
            "488 GTB",
            "488 Pista",
            "488 Spider",
            "550",
            "575",
            "599 GTB",
            "599 GTO",
            "599 SA Aperta",
            "612",
            "812",
            "California",
            "Enzo Ferrari",
            "F12",
            "F355",
            "F40",
            "F430",
            "F50",
            "F8",
            "FF",
            "LaFerrari",
            "GTC4Lusso",
            "Portofino",
            "Purosangue",
            "Roma",
            "SF90",
            "Superamerica",
            "Testarossa",
            "Andere",

            # Koenigsegg
            "Agera",
            "CCR",
            "CCXR",
            "Andere",

            # KTM
            "X-BOW",
            "Andere",

            # Lamborghini
            "Aventador",
            "Countach",
            "Diablo",
            "Gallardo",
            "Huracán",
            "Jalpa",
            "Murciélago",
            "Urus",
            "Andere",

            # McLaren
            "540C",
            "570GT",
            "570S",
            "600LT",
            "620R",
            "650S",
            "650S Coupé",
            "650S Spider",
            "675LT",
            "675LT Spider",
            "720S",
            "765LT",
            "Artura",
            "Elva",
            "GT",
            "MP4-12C",
            "P1",
            "Senna GTR",
            "Speedtail",
            "Andere",

            # Pagani
            "Huayra",
            "Zonda",
            "Andere",

            # Rolls-Royce
            "Flying Spur",
            "Corniche",
            "Cullinan",
            "Dawn",
            "Phantom",
            "Silver Cloud",
            "Silver Dawn",
            "Silver Seraph",
            "Ghost",
            "Silver Shadow",
            "Silver Spirit",
            "Silver Spur",
            "Wraith",
            "Andere",

            # Wiesmann
            "MF 3",
            "MF 35",
            "MF 4",
            "MF 5",

            ###---------------###

            # Cat 2
            # Audi
            "e-tron",
            "quattro",
            "R8",
            "RS2",
            "RS3",
            "RS4",
            "RS5",
            "RS6",
            "RS7",
            "RSQ3",
            "RSQ8",
            "S8",
            "    TT RS",

            ###---------------###

            # Cat 3
            # Mercedes-Benz
            "    A 45 AMG",
            "    AMG GT",
            "    AMG GT C",
            "    AMG GT R",
            "    AMG GT S",
            "    C 63 AMG",
            "    CL 65 AMG",
            "    CLA 45 AMG",
            "    CLA 45 AMG Shooting Brake",
            "    CLK 63 AMG",
            "    CLS 63 AMG",
            "    CLS 63 AMG Shooting Brake",
            "    E 63 AMG",
            "    G 63 AMG",
            "    G 65 AMG",
            "    GL 63 AMG",
            "    GLA 45 AMG",
            "    GLC 63 AMG",
            "    GLE 63 AMG",
            "    GLS 63",
            "    ML 63 AMG",
            "    S 63 AMG",
            "    S 65 AMG",
            "    SL 63 AMG",
            "    SL 65 AMG",
            "SLS AMG",
            "SLR",

            ###---------------###

            # Cat 4
            # BMW
            "    1er M Coupé",
            "    M2",
            "    M3",
            "    M4",
            "    M5",
            "    M6",
            "    M8",
            "    X3 M",
            "    X4 M",
            "    X5 M",
            "    X6 M",
            "    Z3 M",
            "    Z4 M",
            "    Z8",

            ###---------------###

            # Cat 5
            # Corvette
            "C8",
            
            # Dodge
            "Viper",

            # Nissan
            "GT-R",
            
            # Ford
            "GT",

            # Alfa Romeo
            "4C",
            "8C",

            # Jaguar
            "F-Type",
            "Andere",

            # Lexus
            "LFA",

            # Lotus
            "Exige",
            "Emira",
            "Andere",

            # Maserati
            "MC20",

            # Honda
            "NSX",

            ###---------------###

            # Cat 6
            # Porsche
            "356",
            "912",
            "914",
            "918",
            "924",
            "928",
            "    930",
            "944",
            "959",
            "962",
            "    964",
            "968",
            "    991",
            "    992",
            "    993",
            "    996",
            "    997",
            "Boxster",
            "Carrera GT",
            "Cayenne",
            "Cayman",
            "Macan",
            "Panamera",
            "Taycan",
            "Andere",
        ],
        captcha_solver_default="capmonster"
    )

    # Print a status message
    logging.info("The Selenium script finished running. Now, running the Scrapy spider...")

    # Run the spider
    process = CrawlerProcess()
    process.crawl(CarPageSpider)
    process.start()

    # Print a status message
    logging.info("The Scrapy spider finished running. Now, cleaning the data...")

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
    df_data_all_car_brands_cleaned["leistung"] = df_data_all_car_brands_cleaned["leistung"].apply(lambda x: int(re.findall(pattern="(?<=\().*(?=\sPS)", string=x)[0].replace(".", "")) if x is not None else x)
    df_data_all_car_brands_cleaned["preis"] = df_data_all_car_brands_cleaned["preis"].apply(lambda x: int(''.join(re.findall(pattern="\d+", string=x))) if x is not None else x)
    df_data_all_car_brands_cleaned["kilometer"] = df_data_all_car_brands_cleaned["kilometer"].apply(lambda x: int(''.join(re.findall(pattern="\d+", string=x))) if x is not None else x)
    df_data_all_car_brands_cleaned["fahrzeughalter"] = df_data_all_car_brands_cleaned["fahrzeughalter"].apply(lambda x: int(x) if x is not None else x)
    df_data_all_car_brands_cleaned["standort"] = df_data_all_car_brands_cleaned["standort"].apply(lambda x: re.findall(pattern="[A-za-z]+(?=-)", string=x)[0] if x is not None else x)
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
            bigquery.SchemaField("fahrzeugbescheibung", "STRING"),
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
    while True:
        if "mobile_de_scraper\mobile_de" not in os.getcwd():
            os.chdir(os.getcwd() + "/mobile_de")

        # Check if the time is between 11:00 pm and 11:05 pm on a Friday
        if is_between_time_range() or crawl_now == True:
            # Delete any log file that starts with "mobile_logs_cat_all_" and ends with ".log"
            for file in os.listdir():
                if file.startswith("mobile_logs_cat_all_") and file.endswith(".log"):
                    with open(file, 'w') as f:
                        f.close() # Close the file to release the lock
                    os.remove(file)
            # Run the script
            main()
        else:
            # If it's not, wait for 1 minute and check again
            time.sleep(60)
