# Import packages
import json
import os
import time
from datetime import datetime

import requests
from dotenv import load_dotenv
from joblib import Parallel, delayed
from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    TimeoutException,
    WebDriverException
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

# Load environment variables
load_dotenv()

# Define a function to select a "Marke" and a "Modell"
def select_marke_modell(driver, marke, modell):
    # Select the "Marke"
    select1 = Select(driver.find_element(by=By.XPATH, value="//select[@name='makeModelVariant1.makeId']"))
    select1.select_by_visible_text(marke)

    time.sleep(0.5)
    
    # Select the "Modell"
    if modell == "Beliebig":
        pass
    else:
        select2 = Select(driver.find_element(by=By.XPATH, value="//select[@name='makeModelVariant1.modelId']"))
        select2.select_by_visible_text(modell)
    
    # Click on the search button to get re-directed to the search page
    driver.execute_script("document.getElementById('dsp-upper-search-btn').click()")

# Define a function that extracts the URLs of listing pages using Selenium
def extract_listing_page_url(marke, modell):
    # Define the inputs
    base_url = "https://suchen.mobile.de/fahrzeuge/search.html"

    # Define an initial time instance to mark the start of the script
    t1 = datetime.now()
    print(f"The script for {marke} {modell} started at {t1}")

    # Set the chrome options
    chrome_options = Options()
    chrome_options.add_argument('start-maximized') # Required for a maximized Viewport
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation', 'disable-popup-blocking']) # Disable pop-ups to speed up browsing
    chrome_options.add_experimental_option("detach", True)
    chrome_options.add_experimental_option('prefs', {'intl.accept_languages': 'en,en_US'})
    chrome_options.add_argument('--blink-settings=imagesEnabled=false') # Disable images
    chrome_options.add_argument('--disable-extensions') # Disable extensions
    chrome_options.add_argument("--disable-gpu") # Combats the renderer timeout problem
    chrome_options.add_argument("--no-sandbox") # Combats the renderer timeout problem
    chrome_options.add_argument("enable-features=NetworkServiceInProcess") # Combats the renderer timeout problem
    chrome_options.add_argument("disable-features=NetworkService") # Combats the renderer timeout problem
    # chrome_options.add_argument("--headless=new") # Operate Selenium in headless mode
    chrome_options.add_experimental_option('extensionLoadTimeout', 45000) #  Fixes the problem of renderer timeout for a slow PC
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.page_load_strategy = 'eager'

    # Instantiate the driver
    driver = webdriver.Chrome(options=chrome_options)

    # Set page_load_timeout to 45 seconds to avoid renderer timeout
    driver.set_page_load_timeout(45)

    # Navigate to the base URL
    try:
        driver.get(base_url)
        print(f"\nNavigate to the base URL where we can apply the search criteria for {marke} {modell}...")
    except WebDriverException:
        print("\nWebDriverException: Message: unknown error: net::ERR_CONNECTION_RESET\n")
        driver.quit()
        return None

    # Maximize the window
    driver.maximize_window()

    # Wait for "Einverstanden" and click on it
    print(f"Waiting for the Einverstanden window to pop up for {marke} {modell} so we can click on it...")
    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//button[contains(@class,'mde-consent-accept-btn')]")))
        driver.find_element(by=By.XPATH, value="//button[contains(@class,'mde-consent-accept-btn')]").click()
    except TimeoutException:
        print("The Einverstanden/Accept Cookies window did not show up on the apply search criteria page. No need to click on anything...")

    # Apply the filters
    print(f"Applying the search filters for {marke} {modell.strip()}...")
    try:
        select_marke_modell(driver=driver, marke=marke, modell=modell)
    except NoSuchElementException:
        print(f"Marke and Modell chosen {marke} {modell} not found on the page. Potentially check the spelling of the modell. Stopping the driver, returning an empty list and continuing to the next combination...")
        # Stop the driver
        driver.quit()
        return None
    except (ElementClickInterceptedException, WebDriverException) as err:
        print(f"{err} for {marke} {modell.strip()}. Stopping the driver, returning an empty list and continuing to the next combination...")
        # Stop the driver
        driver.quit()
        return None

    # Wait for 3 seconds and extract the URL of the driver
    time.sleep(3)
    output_url = driver.current_url

    # Quit the driver and return the output_url
    driver.quit()
    return marke, modell, output_url

# Define a function that loops over the marke_and_modell_list and extracts the listing page URLs
def parallel_execution_listing_page_url_extractor(inputs):
    # Loop over df_marke_and_modell in parallel and extract the listing page URLs\
    print("Execute the 'extract_listing_page_url' function in parallel to extract the listing page URLs for all brands and models...")
    listing_page_url = Parallel(n_jobs=-1, verbose=10)(
        delayed(extract_listing_page_url)(marke=a, modell=b) for a, b in inputs
    )
    return listing_page_url

# Define the main function that executes the "parallel_execution_listing_page_url_extractor" function
def execute_parallel_crawling(car_list: list, modell_list: list):
    # Load the marke_and_modell JSON file
    with open(file="marke_and_modell_detailed.json", mode="r", encoding="utf-8") as f:
        # Load the JSON file as a list of dicts and close it afterward
        marke_and_modell_lod = json.load(f)
        f.close()

        # Convert the list of dicts to a list of tuples
        marke_and_modell_lot = [(rec["marke"], rec["modell"]) for rec in marke_and_modell_lod]

        # Filter marke_and_modell_list based on the car_list and modell_list
        marke_and_modell_lot = [(marke, modell) for marke, modell in marke_and_modell_lot if marke in car_list and modell in modell_list]

    results = parallel_execution_listing_page_url_extractor(marke_and_modell_lot)

    # Save the results to a JSON file
    with open("target_url_list_cat_all.json", mode="w", encoding="utf-8") as f:
        json.dump(obj=results, fp=f, ensure_ascii=False, indent=4)

execute_parallel_crawling(
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
        ]
)