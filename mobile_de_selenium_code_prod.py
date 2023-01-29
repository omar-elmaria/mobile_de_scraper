from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from twocaptcha import TwoCaptcha
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
import os
import time
import json
import re
import pandas as pd

# Step 1: Load environment variables
load_dotenv()

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
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
chrome_options.add_experimental_option("detach", True)
chrome_options.add_experimental_option('prefs', {'intl.accept_languages': 'en,en_US'})
# chrome_options.add_argument("--headless=new") # Operate Selenium in headless mode
# chrome_options.add_argument('--no-sandbox')
# chrome_options.add_argument('--disable-gpu')
# chrome_options.add_argument("--window-size=1920x1080")

# Step 5: Instantiate a browser object and navigate to the URL
driver = webdriver.Chrome(chrome_options=chrome_options)

# Step 6: Define a function to solve the captcha using the 2captcha service
def solve_captcha(sitekey, url):
    try:
        result = solver.recaptcha(sitekey=sitekey, url=url)
        captcha_key = result.get('code')
        print(f"Captcha solved. The key is: {captcha_key}\n")
    except Exception as e:
        print(f"Captcha not solved...")
        captcha_key = None
        exit(e)

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
def handle_none_elements(command):
    try:
        return command
    except NoSuchElementException as err:
        print(err)
        return None

# Step 9.1: Navigate to the base URL from which we will start our search
driver.get(base_url)
print("\nNavigating to the base URL where we can apply the search criteria...")

# Step 9.2: Maximize the window
driver.maximize_window()

# Step 9.3: Wait for "Einverstanden" and click on it
print("Waiting for the Einverstanden window to pop up so we can click on it...")
WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']")))
driver.find_element(by=By.XPATH, value="//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']").click()

# Step 9.4: Apply the filters
print("Applying the search filters...")
select_marke_modell(marke=marke_and_modell_list[3]["marke"], modell=marke_and_modell_list[3]["modell"])

# Step 9.5: Solve the captcha
print("Applied the search filters. Now, solving the captcha...")
captcha_key = solve_captcha(sitekey=sitekey, url=driver.current_url)

# Step 9.6: If the a captcha token was returned, invoke the callback function and navigate to the results page
if captcha_key is not None:
    # html of the captcha is inside an iframe, selenium cannot see it if we first don't switch to the iframe
    WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "sec-cpt-if")))

    # Inject the token into the inner HTML of g-recaptcha-response and invoke the callback function
    driver.execute_script(f'document.getElementById("g-recaptcha-response").innerHTML="{captcha_key}"')
    driver.execute_script(f"verifyAkReCaptcha('{captcha_key}')") # This step fails in Python but runs successfully in the console

    # Wait for "Einverstanden" and click on it
    print("Waiting to see if the Einverstanden window pops up again after landing on the results page...")
    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']")))
        driver.find_element(by=By.XPATH, value="//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']").click()
    except TimeoutException:
        print("The Einverstanden/Accept Cookies window did not show up. No need to click on anything...")

    # Wait for 0.5 seconds until the page is loaded
    time.sleep(0.5)

    # Print the top title of the page
    tot_search_results = re.findall(pattern="\d+", string=driver.find_element(by=By.XPATH, value="//h1[@data-testid='result-list-headline']").text)[0]
    print(f"The results page has been retrieved. In total, we have {tot_search_results} listings to loop through...")
else:
    print("The captcha was not solved for the marke and modell chosen. Continuing to the next combination...")

# Step 10: We are at the results page now. We need to crawl the links to the individual car pages
# Step 10.1: Get the landing page URL and last page of the brand-model combination
landing_page_url = driver.current_url
last_page_web_element_list = driver.find_elements(by=By.XPATH, value="//span[@class='btn btn--secondary btn--l']")
last_page = int(last_page_web_element_list[-1].text)
print(f"We have a total of {last_page} pages to loop through for the chosen marke and modell...")

# Step 10.2: Loop through all the pages of the "marke" and "modell" combination
all_brands_data_list = []
for pg in range(2, last_page + 2):
    # Step 10.2.1: Get all the car URLs on the page. Don't crawl the "sponsored" or the "top in category" listings 
    print(f"Crawling the car links on page {pg - 1}...")
    car_web_elements = driver.find_elements(by=By.XPATH, value="//div[contains(@class, 'cBox-body cBox-body') and @class!='cBox-body cBox-body--topInCategory' and @class!='cBox-body cBox-body--topResultitem']")
    car_page_url_list = []
    for web in car_web_elements:
        car_page_url = web.find_element(by=By.XPATH, value="./a").get_attribute("href")
        car_page_url_list.append(car_page_url)
    
    # Step 10.2.2: Navigate to each individual car page and crawl the data
    one_brand_data_list = []
    for idx, i in enumerate(car_page_url_list):
        print(f"Navigating to car page {idx + 1} out of {len(car_web_elements)} on page {pg - 1} out of {last_page}")
        driver.get(i)

        # Step 10.2.3: Extract the vehicle data
        # Extract the vehicle description and join it
        # fahrzeug_beschreibung = [x.get_attribute("text") for x in driver.find_elements(by=By.XPATH, value="//div[@class='g-col-12 description']")]
        # if fahrzeug_beschreibung is not None:
        #     fahrzeug_beschreibung = "\n".join(fahrzeug_beschreibung)
        # else:
        #     fahrzeug_beschreibung = None

        output_dict = {
            "marke": "Ferrari",
            "modell": "458",
            "variante": "",
            "titel": handle_none_elements(command=driver.find_element(by=By.XPATH, value="//h1[@id='ad-title']").text) + " " + handle_none_elements(driver.find_element(by=By.XPATH, value="//div[@class='listing-subtitle']").text),
            "form": handle_none_elements(command=driver.find_element(by=By.XPATH, value="//div[@id='category-v']").text),
            "fahrzeugzustand": handle_none_elements(command=driver.find_element(by=By.XPATH, value="//div[@id='damageCondition-v']").text),
            'leistung': handle_none_elements(command=driver.find_element(by=By.XPATH, value="//div[text()='Leistung']/following-sibling::div").text),
            'getriebe': handle_none_elements(command=driver.find_element(by=By.XPATH, value="//div[text()='Getriebe']/following-sibling::div").text),
            "farbe": handle_none_elements(command=driver.find_element(by=By.XPATH, value="//div[@id='color-v']").text),
            "preis": handle_none_elements(command=driver.find_element(by=By.XPATH, value="//span[@data-testid='prime-price']")),
            "kilometer": handle_none_elements(command=driver.find_element(by=By.XPATH, value="//div[text()='Kilometerstand']/following-sibling::div").text),
            "erstzulassung": handle_none_elements(command=driver.find_element(by=By.XPATH, value="//div[text()='Erstzulassung']/following-sibling::div").text),
            "fahrzeughalter": handle_none_elements(command=driver.find_element(by=By.XPATH, value="//div[text()='Fahrzeughalter']/following-sibling::div").text),
            "standort": handle_none_elements(command=driver.find_element(by=By.XPATH, value="//p[@id='seller-address']").text),
            # "fahrzeugbescheibung": fahrzeug_beschreibung,
            "url_to_crawl": i,
            "page_rank": pg - 1,
            "total_num_pages": last_page,
        }
        one_brand_data_list.append(output_dict)
    df_one_brand_data = pd.concat(one_brand_data_list)

    # Append the results to "all_brands_data_list"
    all_brands_data_list.append(df_one_brand_data)

    # Write the results to a JSON file
    with open("df_all_brands_data.json", mode="w", encoding="utf-8") as f:
        json.dump(obj=all_brands_data_list, fp=f, ensure_ascii=False, indent=0)
    
    # Step 10.2.3: Navigate to the next page
    if pg <= last_page:
        print(f"\nMoving to the next page, page {pg}")
        driver.get(landing_page_url + f"&pageNumber={pg}")
    else:
        print("Reached the end of the results page...")