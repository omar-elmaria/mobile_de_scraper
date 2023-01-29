from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from twocaptcha import TwoCaptcha
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
import os
import time
import json
import re
import pandas as pd
from datetime import datetime

# Step 1: Load environment variables and define an initial time instance to mark the start of the script
load_dotenv()
t1 = datetime.now()
print(f"The script started at {t1}")

# Step 2: Load the marke_and_modell JSON file
with open(file="marke_and_modell.json", mode="r", encoding="utf-8") as f:
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
# chrome_options.add_argument("--headless=new") # Operate Selenium in headless mode
# chrome_options.add_argument('--no-sandbox')
# chrome_options.add_argument('--disable-gpu')
# chrome_options.add_argument("--window-size=1920x1080")

# Step 5: Instantiate a browser object and navigate to the URL
capabibilties = DesiredCapabilities().CHROME
capabibilties['pageLoadStrategy'] = 'eager' # Eafer pageLoadStrategy is used to speed up browsing
driver = webdriver.Chrome(desired_capabilities=capabibilties, chrome_options=chrome_options)

# Step 6: Define a function to solve the captcha using the 2captcha service
def solve_captcha(sitekey, url):
    try:
        result = solver.recaptcha(sitekey=sitekey, url=url)
        captcha_key = result.get('code')
        print(f"Captcha solved. The key is: {captcha_key}\n")
    except Exception as e:
        print(f"Captcha not solved...")
        captcha_key = None

    return captcha_key

# Step 7: Define a function to select a "Marke" and a "Modell"
def select_marke_modell(marke, modell):
    # Select the "Marke"
    select1 = Select(driver.find_element(by=By.XPATH, value="//select[@name='makeModelVariant1.makeId']"))
    select1.select_by_visible_text(marke)
    
    # Select the "Modell"
    if modell == "Beliebig":
        pass
    else:
        select2 = Select(driver.find_element(by=By.XPATH, value="//select[@name='makeModelVariant1.modelId']"))
        select2.select_by_visible_text(modell)
    
    # Click on the search button to get re-directed to the search page
    driver.find_element(by=By.XPATH, value="//button[@id='dsp-upper-search-btn']").click()

# Step 8: Define a function to handle parsing errors on individual car pages
def handle_none_elements(xpath):
    try:
        web_element = driver.find_element(by=By.XPATH, value=xpath)
    except NoSuchElementException as err:
        print(f"This xpath --> {xpath} was not found. Setting the result to None...")
        return ""
    
    return web_element.text

# Define a function to navigate to the base URL, apply the search filters, bypass the captcha, crawl the data and return it to a JSON file
def crawl_func(dict_idx):
    # Step 9.0: Get the marke and modell based on the dict_idx 
    marke = marke_and_modell_list[dict_idx]["marke"]
    modell = marke_and_modell_list[dict_idx]["modell"]

    # Step 9.1: Navigate to the base URL from which we will start our search
    driver.get(base_url)
    print("\nNavigating to the base URL where we can apply the search criteria...")

    # Step 9.2: Maximize the window
    driver.maximize_window()

    # Step 9.3: Wait for "Einverstanden" and click on it
    print("Waiting for the Einverstanden window to pop up so we can click on it...")
    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']")))
        driver.find_element(by=By.XPATH, value="//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']").click()
    except TimeoutException:
        print("The Einverstanden/Accept Cookies window did not show up on the apply search criteria page. No need to click on anything...")

    # Step 9.4: Apply the filters
    print(f"Applying the search filters for {marke} {modell}...")
    select_marke_modell(marke=marke, modell=modell)

    # Step 9.5: Solve the captcha
    print(f"Applied the search filters for {marke} {modell}. Now, solving the captcha...")
    captcha_key = solve_captcha(sitekey=sitekey, url=driver.current_url)

    # Step 9.6: If the a captcha token was returned, invoke the callback function and navigate to the results page
    if captcha_key is not None:
        # html of the captcha is inside an iframe, selenium cannot see it if we first don't switch to the iframe
        WebDriverWait(driver, 15).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "sec-cpt-if")))

        # Inject the token into the inner HTML of g-recaptcha-response and invoke the callback function
        driver.execute_script(f'document.getElementById("g-recaptcha-response").innerHTML="{captcha_key}"')
        driver.execute_script(f"verifyAkReCaptcha('{captcha_key}')") # This step fails in Python but runs successfully in the console

        # Wait for "Einverstanden" and click on it
        print("Waiting to see if the Einverstanden window pops up again after landing on the results page...")
        try:
            WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, "//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']")))
            driver.find_element(by=By.XPATH, value="//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']").click()
        except TimeoutException:
            print("The Einverstanden/Accept Cookies window did not show up on the results page. No need to click on anything...")

        # Wait for 0.5 seconds until the page is loaded
        time.sleep(0.5)

        # Print the top title of the page
        tot_search_results = re.findall(pattern="\d+", string=driver.find_element(by=By.XPATH, value="//h1[@data-testid='result-list-headline']").text)[0]
        print(f"The results page of {marke} {modell} has been retrieved. In total, we have {tot_search_results} listings to loop through...")
    else:
        print(f"The captcha was not solved for the marke and modell chosen ({marke} {modell}). Continuing to the next combination...")

    # Step 10: We are at the results page now. We need to crawl the links to the individual car pages
    # Step 10.1: Get the landing page URL and last page of the brand-model combination
    landing_page_url = driver.current_url
    last_page_web_element_list = driver.find_elements(by=By.XPATH, value="//span[@class='btn btn--secondary btn--l']")
    try:
        last_page = int(last_page_web_element_list[-1].text)
    except IndexError: # The index error can occur if the brand has only one page. In that case, set last_page to 1
        last_page = 1
    print(f"We have a total of {last_page} pages under {marke} {modell} to loop through...")

    # Step 10.2: Loop through all the pages of the "marke" and "modell" combination
    all_pages_data_list = []
    for pg in range(2, last_page + 2):
        # Step 10.2.1: Get all the car URLs on the page. Don't crawl the "sponsored" or the "top in category" listings 
        print(f"Crawling the car links on page {pg - 1}...")
        car_web_elements = driver.find_elements(by=By.XPATH, value="//div[contains(@class, 'cBox-body cBox-body') and @class!='cBox-body cBox-body--topInCategory' and @class!='cBox-body cBox-body--topResultitem']")
        car_page_url_list = []
        for web in car_web_elements:
            car_page_url = web.find_element(by=By.XPATH, value="./a").get_attribute("href")
            car_page_url_list.append(car_page_url)
        
        # Step 10.2.2: Navigate to each individual car page and crawl the data
        one_page_data_list = []
        for idx, i in enumerate(car_page_url_list):
            print(f"Navigating to car page {idx + 1} out of {len(car_web_elements)} on page {pg - 1} out of {last_page}")
            driver.get(i)

            # Sometimes a pop-up appears asking the user to fill in a survey or share their satisfaction with the website. This command handles this situation
            try: # Survey pop-up
                WebDriverWait(driver, 1.25).until(EC.presence_of_element_located((By.XPATH, "//input[@id='neinDankeDCoreOverlay']")))
                driver.find_element(by=By.XPATH, value="//input[@id='neinDankeDCoreOverlay']").click()
            except TimeoutException:
                print("No survey pop-up found. Continuing as usual...")
            
            try: # Satisfaction pop-up
                WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.XPATH, "//button[@data-testid='ces:modal:close']")))
                driver.find_element(by=By.XPATH, value="//button[@data-testid='ces:modal:close']").click()
            except TimeoutException:
                print("No satisfaction pop-up found. Continuing as usual...")

            # Step 10.2.3: Extract the vehicle data
            # Extract the vehicle description
            try:
                fahrzeug_beschreibung = driver.find_element(by=By.XPATH, value="//div[@class='g-col-12 description']").get_attribute("textContent")
            except NoSuchElementException:
                fahrzeug_beschreibung = ""

            output_dict = {
                "marke": marke,
                "modell": modell,
                "variante": "",
                "titel": handle_none_elements(xpath="//h1[@id='ad-title']") + " " + handle_none_elements("//div[@class='listing-subtitle']"),
                "form": handle_none_elements(xpath="//div[@id='category-v']"),
                "fahrzeugzustand": handle_none_elements(xpath="//div[@id='damageCondition-v']"),
                'leistung': handle_none_elements(xpath="//div[text()='Leistung']/following-sibling::div"),
                'getriebe': handle_none_elements(xpath="//div[text()='Getriebe']/following-sibling::div"),
                "farbe": handle_none_elements(xpath="//div[@id='color-v']"),
                "preis": handle_none_elements(xpath="//span[@data-testid='prime-price']"),
                "kilometer": handle_none_elements(xpath="//div[text()='Kilometerstand']/following-sibling::div"),
                "erstzulassung": handle_none_elements(xpath="//div[text()='Erstzulassung']/following-sibling::div"),
                "fahrzeughalter": handle_none_elements(xpath="//div[text()='Fahrzeughalter']/following-sibling::div"),
                "standort": handle_none_elements(xpath="//p[@id='seller-address']"),
                "fahrzeugbescheibung": fahrzeug_beschreibung,
                "url_to_crawl": i,
                "page_rank": pg - 1,
                "total_num_pages": last_page,
            }
            one_page_data_list.append(output_dict)

        # Append the results to "all_pages_data_list"
        all_pages_data_list.extend(one_page_data_list)

        # Write the results to a JSON file
        with open("df_all_brands_data.json", mode="a", encoding="utf-8") as f:
            json.dump(obj=all_pages_data_list, fp=f, ensure_ascii=False, indent=4)
        
        # Step 10.2.3: Navigate to the next page
        if pg <= last_page:
            print(f"\nMoving to the next page, page {pg}")
            driver.get(landing_page_url + f"&pageNumber={pg}")
        else:
            print(f"Reached the end of the results page for {marke} {modell}...")

# Step 11: Loop through all the brands in the JSON file
for idx, rec in enumerate(marke_and_modell_list):
    if rec["marke"] in ["ALPINA", "Aston Martin", "Bentley", "Ferrari", "Lamborghini", "Maybach", "McLaren", "Porsche", "Rolls Royce"]:
        continue
    else:
        crawl_func(dict_idx=idx)

# Step 12: Open the JSON file containing all car brands and convert it into a pandas data frame
with open("df_all_brands_data.json", mode="r", encoding="utf-8") as f:
    data = json.load(f)
    f.close()

df_data_all_car_brands = []
for i in data: # Loop through every page
    for j in i: # Loop through each car listing on a specific page
        df_data_all_car_brands.append(j)

df_data_all_car_brands = pd.DataFrame(df_data_all_car_brands)

# Print the head of the data frame
print(df_data_all_car_brands.head())

# Print a status message marking the end of the script
t2 = datetime.now()
print(f"The script finished at {t2}. It took {t2-t1} to crawl all listings...")