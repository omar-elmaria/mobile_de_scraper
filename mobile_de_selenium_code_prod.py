from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from twocaptcha import TwoCaptcha
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
import os
import time
import json

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
chrome_options.add_argument("--headless=new") # Operate Selenium in headless mode
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument("--window-size=1920x1080")

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

# Step 8.1: Navigate to the base URL from which we will start our search
driver.get(base_url)
print("Navigating to the base URL where we can apply the search criteria...")

# Step 8.2: Maximize the window
driver.maximize_window()

# Step 8.3: Wait for "Einverstanden" and click on it
print("Waiting for the Einverstanden window to pop up so we can click on it...")
WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']")))
driver.find_element(by=By.XPATH, value="//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']").click()

# Step 8.4: Apply the filters
print("Applying the search filters...")
select_marke_modell(marke=marke_and_modell_list[0]["marke"], modell=marke_and_modell_list[0]["modell"])

# Step 8.5: Solve the captcha
print("Applied the search filters. Now, solving the captcha...")
captcha_key = solve_captcha(sitekey=sitekey, url=driver.current_url)

# Step 8.6: If the a captcha token was returned, invoke the callback function and navigate to the results page
if captcha_key is not None:
    # html of the captcha is inside an iframe, selenium cannot see it if we first don't switch to the iframe
    WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "sec-cpt-if")))

    # Inject the token into the inner HTML of g-recaptcha-response and invoke the callback function
    driver.execute_script(f'document.getElementById("g-recaptcha-response").innerHTML="{captcha_key}"')
    driver.execute_script(f"verifyAkReCaptcha('{captcha_key}')") # This step fails in Python but runs successfully in the console

    # Wait for "Einverstanden" and click on it
    print("Waiting to see if the Einverstanden window pops up again after applying the filters and solving the captcha...")
    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']")))
        driver.find_element(by=By.XPATH, value="//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']").click()
    except TimeoutException:
        print("THe Einverstanden/Accept Cookies window did not show up. No need to click on anything...")

    # Wait for 0.5 seconds until the page is loaded
    time.sleep(0.5)

    # Print the top title of the page
    print(driver.find_element(by=By.XPATH, value="//h1[@data-testid='result-list-headline']").text)
else:
    print("The captcha was not solved for the marke and modell chosen. Continuing to the next combination...")

# Step 9: We are at the results page now. We need to crawl the links to the individual car pages
# Step 9.1: Get the last page of the brand-model combination
last_page = int(driver.find_element(by=By.XPATH, value="//li[@class='padding-last-button']/span").text)

# Loop through all the pages of the "marke" and "modell" combination
for pg in range(1, last_page + 1):
    driver.get(driver.current_url + f"&pageNumber={pg}")

    # Don't crawl the "sponsored" or the "top in category" listings 
    cars = driver.find_elements(by=By.XPATH, value="//div[contains(@class, 'cBox-body cBox-body') and @class!='cBox-body cBox-body--topInCategory' and @class!='cBox-body cBox-body--topResultitem']")
    for car in cars:
        driver.get(driver.find_element(by=By.XPATH, value="./a").get_attribute("href"))