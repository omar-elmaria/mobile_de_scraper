from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from twocaptcha import TwoCaptcha
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from dotenv import load_dotenv
import os
import time
import json
import re
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from datetime import datetime
import yagmail

# Step 1: Load environment variables and define an initial time instance to mark the start of the script
load_dotenv()
t1 = datetime.now()
print(f"The script started at {t1}")

# Step 2: Load the marke_and_modell JSON file
with open(file="marke_and_modell_detailed.json", mode="r", encoding="utf-8") as f:
    marke_and_modell_list = json.load(f)
    f.close()

# Step 3: Instantiate a Captcha solver object and define the sitekey and base URL
solver = TwoCaptcha(os.getenv("CAPTCHA_API_KEY"))
sitekey = "6Lfwdy4UAAAAAGDE3YfNHIT98j8R1BW1yIn7j8Ka"
base_url = "https://suchen.mobile.de/fahrzeuge/search.html"

# Step 4: Set the chrome options
chrome_options = Options()
chrome_options.add_argument('start-maximized') # Required for a maximized Viewport
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation', 'disable-popup-blocking']) # Disable pop-ups to speed up browsing
chrome_options.add_experimental_option("detach", True)
chrome_options.add_experimental_option('prefs', {'intl.accept_languages': 'en,en_US'})
chrome_options.add_argument('--blink-settings=imagesEnabled=false') # Disable images
chrome_options.add_argument('--disable-extensions') # Disable extensions
chrome_options.add_argument("--headless=new") # Operate Selenium in headless mode
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument("--window-size=1920x1080")

# Step 5: Instantiate a browser object and navigate to the URL
capabibilties = DesiredCapabilities().CHROME
capabibilties['pageLoadStrategy'] = 'eager' # Eafer pageLoadStrategy is used to speed up browsing

# Step 6: Define a function to solve the captcha using the 2captcha service
def solve_captcha(sitekey, url):
    try:
        result = solver.recaptcha(sitekey=sitekey, url=url)
        captcha_key = result.get('code')
        print(f"Captcha solved. The key is: {captcha_key}\n")
    except Exception:
        print(f"Captcha not solved...")
        captcha_key = None

    return captcha_key

# Step 7: Define a function to select a "Marke" and a "Modell"
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
    driver.find_element(by=By.XPATH, value="//button[@id='dsp-upper-search-btn']").click()

# Step 8: Define a function to handle parsing errors on individual car pages
def handle_none_elements(driver, xpath):
    try:
        web_element = driver.find_element(by=By.XPATH, value=xpath)
    except NoSuchElementException:
        print(f"This xpath --> {xpath} was not found. Setting the result to None...")
        return ""
    
    return web_element.text

# Step 9: Define a function to invoke the callback function
def invoke_callback_func(driver, captcha_key):
    try: # Sometimes the captcha is solved without having to invoke the callback function. This piece of code handles this situation
        # html of the captcha is inside an iframe, selenium cannot see it if we first don't switch to the iframe
        WebDriverWait(driver, 5).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "sec-cpt-if")))

        # Inject the token into the inner HTML of g-recaptcha-response and invoke the callback function
        driver.execute_script(f'document.getElementById("g-recaptcha-response").innerHTML="{captcha_key}"')
        driver.execute_script(f"verifyAkReCaptcha('{captcha_key}')") # This step fails in Python but runs successfully in the console
    except TimeoutException:
        print("Captcha was solved without needing to invoke the callback function. Bypassing this part of the script to prevent raising an error")

    # Wait for "Einverstanden" and click on it
    print("Waiting to see if the Einverstanden window pops up again after landing on the results page...")
    try:
        WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, "//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']")))
        driver.find_element(by=By.XPATH, value="//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']").click()
    except TimeoutException:
        print("The Einverstanden/Accept Cookies window did not show up on the results page. No need to click on anything...")

    # Wait for 0.5 seconds until the page is loaded
    time.sleep(0.5)

# Step 10: Define a function to navigate to the base URL, apply the search filters, bypass the captcha, crawl the data and return it to a JSON file
def crawl_func(dict_idx):
    # Instantiate the chrome driver
    driver = webdriver.Chrome(desired_capabilities=capabibilties, chrome_options=chrome_options)

    # Step 10.0: Get the marke and modell based on the dict_idx 
    marke = marke_and_modell_list[dict_idx]["marke"]
    modell = marke_and_modell_list[dict_idx]["modell"]

    # Step 10.1: Navigate to the base URL from which we will start our search
    driver.get(base_url)
    print("\nNavigating to the base URL where we can apply the search criteria...")

    # Step 10.2: Maximize the window
    driver.maximize_window()

    # Step 10.3: Wait for "Einverstanden" and click on it
    print("Waiting for the Einverstanden window to pop up so we can click on it...")
    try:
        WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, "//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']")))
        driver.find_element(by=By.XPATH, value="//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']").click()
    except TimeoutException:
        print("The Einverstanden/Accept Cookies window did not show up on the apply search criteria page. No need to click on anything...")

    # Step 10.4: Apply the filters
    print(f"Applying the search filters for {marke} {modell}...")
    select_marke_modell(driver=driver, marke=marke, modell=modell)

    # Step 10.5: Solve the captcha
    print(f"Applied the search filters for {marke} {modell}. Now, solving the captcha...")
    captcha_key = solve_captcha(sitekey=sitekey, url=driver.current_url)

    # Step 10.6: If the a captcha token was returned, invoke the callback function and navigate to the results page
    if captcha_key is not None:
        # Invoke the callback function
        invoke_callback_func(driver=driver, captcha_key=captcha_key)

        # Print the top title of the page
        tot_search_results = re.findall(pattern="\d+", string=driver.find_element(by=By.XPATH, value="//h1[@data-testid='result-list-headline']").text)[0]
        print(f"The results page of {marke} {modell} has been retrieved. In total, we have {tot_search_results} listings to loop through...")

        # Continue on with the rest of the crawling
        # Step 11: We are at the results page now. We need to crawl the links to the individual car pages
        # Step 11.1: Get the landing page URL and last page of the brand-model combination
        landing_page_url = driver.current_url
        last_page_web_element_list = driver.find_elements(by=By.XPATH, value="//span[@class='btn btn--secondary btn--l']")
        try:
            last_page = int(last_page_web_element_list[-1].text)
        except IndexError: # The index error can occur if the brand has only one page. In that case, set last_page to 1
            last_page = 1
        print(f"We have a total of {last_page} pages under {marke} {modell} to loop through...")

        # Step 11.2: Loop through all the pages of the "marke" and "modell" combination and crawl the individual car links that contain the information we want to crawl
        car_page_url_list = []
        for pg in range(2, last_page + 2):
            # Step 11.2.1: Get all the car URLs on the page. Don't crawl the "sponsored" or the "top in category" listings 
            print(f"Crawling the car links on page {pg - 1}...")
            car_web_elements = driver.find_elements(by=By.XPATH, value="//div[contains(@class, 'cBox-body cBox-body') and @class!='cBox-body cBox-body--topInCategory' and @class!='cBox-body cBox-body--topResultitem']")
            for web in car_web_elements:
                car_page_url = web.find_element(by=By.XPATH, value="./a").get_attribute("href")
                car_page_url_list.append(car_page_url)
            
            # Step 11.2.2: Navigate to the next page to collect the next batch of URLs
            if pg <= last_page:
                print(f"\nMoving to the next page, page {pg}")
                driver.get(landing_page_url + f"&pageNumber={pg}")
                time.sleep(1)
                # Sometimes, a captcha is shown after navigating to the next page under of a car brand. We need to invoke the captcha service here if that happens
                try:
                    # Check for the existence of the "Angebote entsprechen Deinen Suchkriterien" header
                    driver.find_element(by=By.XPATH, value="//h1[@data-testid='result-list-headline']").text
                    # If the header doesn't exist, proceed normally to the next page
                    print(f"No Captcha was found after navigating to page {pg} under {marke} {modell}. Proceeding normally...")
                except NoSuchElementException:
                    print(f"Captcha found while navigating to page {pg} under {marke} {modell}. Solving it with the 2captcha service...")

                    # If there was a raised exception, this means that the header does not exist, so invoke the solve_captcha function
                    captcha_token = solve_captcha(sitekey=sitekey, url=driver.current_url)

                    # Invoke the callback function
                    invoke_callback_func(driver=driver, captcha_key=captcha_token)
            else:
                print(f"Crawled all the car links of {marke} {modell}...")
            
        # Step 11.3.1: Disable Javascript to prevent ads from popping up
        path = 'chrome://settings/content/javascript'
        driver.get(path)
        # clicking toggle button
        time.sleep(1)
        ActionChains(driver).send_keys(Keys.TAB).send_keys(Keys.TAB).send_keys(Keys.DOWN).perform()
        
        # Step 11.3.2: Navigate to each individual car page and crawl the data
        all_pages_data_list = []
        for idx, i in enumerate(car_page_url_list):
            print(f"{len(car_page_url_list)} links were crawled under {marke} {modell}. Navigating to car #{idx + 1} out of {len(car_page_url_list)}...")
            driver.get(i)

            # Step 11.3.1: Extract the vehicle data
            # Extract the vehicle description
            try:
                fahrzeug_beschreibung = driver.find_element(by=By.XPATH, value="//div[@class='g-col-12 description']").get_attribute("textContent")
            except NoSuchElementException:
                print(f"This xpath --> //div[@class='g-col-12 description'] was not found. Setting the result to None...")
                fahrzeug_beschreibung = ""
            
            # Extract the color as it does not follow the regular format of handle_none_elements func
            try:
                farbe = driver.find_element(by=By.XPATH, value="//div[@id='color-v']").get_attribute("textContent")
            except NoSuchElementException:
                farbe = ""
                print("farbe not found")
            
            output_dict = {
                "marke": marke,
                "modell": modell,
                "variante": "",
                "titel": handle_none_elements(driver=driver, xpath="//h1[@id='ad-title']") + " " + handle_none_elements(driver=driver, xpath="//div[@class='listing-subtitle']"),
                "form": handle_none_elements(driver=driver, xpath="//div[@id='category-v']"),
                "fahrzeugzustand": handle_none_elements(driver=driver, xpath="//div[@id='damageCondition-v']"),
                "leistung": handle_none_elements(driver=driver, xpath="//div[text()='Leistung']/following-sibling::div"),
                "getriebe": handle_none_elements(driver=driver, xpath="//div[text()='Getriebe']/following-sibling::div"),
                "farbe": farbe,
                "preis": handle_none_elements(driver=driver, xpath="//span[@data-testid='prime-price']"),
                "kilometer": handle_none_elements(driver=driver, xpath="//div[text()='Kilometerstand']/following-sibling::div"),
                "erstzulassung": handle_none_elements(driver=driver, xpath="//div[text()='Erstzulassung']/following-sibling::div"),
                "fahrzeughalter": handle_none_elements(driver=driver, xpath="//div[text()='Fahrzeughalter']/following-sibling::div"),
                "standort": handle_none_elements(driver=driver, xpath="//p[@id='seller-address']"),
                "fahrzeugbescheibung": fahrzeug_beschreibung,
                "url_to_crawl": i,
                "page_rank": pg - 1,
                "total_num_pages": last_page
            }
            all_pages_data_list.append(output_dict)
        # Stop the driver
        driver.quit()
        return all_pages_data_list
    else:
        # If the captcha solver did not return a token, return an empty list and proceed to the next marke-modell combination
        print(f"The captcha was not solved for the marke and modell chosen ({marke} {modell}). Continuing to the next combination...")
        # Stop the driver
        driver.quit()
        return []

# Step 12: Loop through all the brands in the JSON file
all_brands_data_list = []
for idx, rec in enumerate(marke_and_modell_list):
    if rec["marke"] not in ["Koenigsegg"]:
        continue
    else:
        all_brands_data_list.append(crawl_func(dict_idx=idx))
        # Write the results to a JSON file
        with open("df_all_brands_data.json", mode="w", encoding="utf-8") as f:
            json.dump(obj=all_brands_data_list, fp=f, ensure_ascii=False, indent=4)

# Step 13: Open the JSON file containing all car brands and convert it into a pandas data frame
with open("df_all_brands_data.json", mode="r", encoding="utf-8") as f:
    data = json.load(f)
    f.close()

df_data_all_car_brands = []
for i in data: # Loop through every page
    for j in i: # Loop through each car listing on a specific page
        df_data_all_car_brands.append(j)

df_data_all_car_brands = pd.DataFrame(df_data_all_car_brands)

# Print the head of the data frame
print(df_data_all_car_brands.head(10))

# Step 14: Clean the data
df_data_all_car_brands_cleaned = df_data_all_car_brands.copy()
df_data_all_car_brands_cleaned.replace(to_replace="", value=None, inplace=True)
df_data_all_car_brands_cleaned["leistung"] = df_data_all_car_brands_cleaned["leistung"].apply(lambda x: int(re.findall(pattern="(?<=\().*(?=\sPS)", string=x)[0].replace(".", "")) if x is not None else x)
df_data_all_car_brands_cleaned["preis"] = df_data_all_car_brands_cleaned["preis"].apply(lambda x: int(''.join(re.findall(pattern="\d+", string=x))) if x is not None else x)
df_data_all_car_brands_cleaned["kilometer"] = df_data_all_car_brands_cleaned["kilometer"].apply(lambda x: int(''.join(re.findall(pattern="\d+", string=x))) if x is not None else x)
df_data_all_car_brands_cleaned["fahrzeughalter"] = df_data_all_car_brands_cleaned["fahrzeughalter"].apply(lambda x: int(x) if x is not None else x)
df_data_all_car_brands_cleaned["standort"] = df_data_all_car_brands_cleaned["standort"].apply(lambda x: re.findall(pattern="[A-za-z]+(?=-)", string=x)[0] if x is not None else x)
df_data_all_car_brands_cleaned["crawled_timestamp"] = datetime.now()
print(df_data_all_car_brands.head(10))

# Step 15: Upload to bigquery
# First, set the credentials
key_path = os.getcwd() + "/bq_credentials.json"
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
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
job = client.load_table_from_dataframe(
    dataframe=df_data_all_car_brands_cleaned,
    destination="web-scraping-371310.crawled_datasets.lukas_mobile_de",
    job_config=job_config
).result()

# Step 16: Send success E-mail
yag = yagmail.SMTP("omarmoataz6@gmail.com", oauth2_file=os.getcwd()+"/email_authentication.json")
contents = [
    "This is an automated notification to inform you that the mobile.de scraper ran successfully"
]
yag.send(["omarmoataz6@gmail.com"], f"The Mobile.de Scraper Ran Successfully on {datetime.now()} CET", contents)

# Print a status message marking the end of the script
t2 = datetime.now()
print(f"The script finished at {t2}. It took {t2-t1} to crawl all listings...")