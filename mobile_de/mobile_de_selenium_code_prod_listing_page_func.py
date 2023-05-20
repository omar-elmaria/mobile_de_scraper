import json
import os
import re
import time
from datetime import datetime

from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    InvalidArgumentException,
    JavascriptException,
    NoSuchElementException,
    TimeoutException,
    WebDriverException
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from twocaptcha import TwoCaptcha


def mobile_de_local_single_func(category: str, car_list: list, modell_list: list):
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s  - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename=f"mobile_logs_{category}.log"
    )

    # Step 1: Load environment variables and define an initial time instance to mark the start of the script
    load_dotenv()
    t1 = datetime.now()
    logging.info(f"The script started at {t1}")

    # Step 2: Load the marke_and_modell JSON file
    with open(file="marke_and_modell_detailed.json", mode="r", encoding="utf-8") as f:
        marke_and_modell_list = json.load(f)
        f.close()

    # Step 3: Instantiate a Captcha solver object and define the sitekey and base URL
    solver = TwoCaptcha(os.getenv("CAPTCHA_API_KEY"))
    sitekey = "6Lfwdy4UAAAAAGDE3YfNHIT98j8R1BW1yIn7j8Ka"
    base_url = "https://suchen.mobile.de/fahrzeuge/search.html"

    # Step 4: Set the chrome options (adding --no-sandbox makes thing worse security-wise; removing --disable-gpu makes things worse as well security-wise)
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
    chrome_options.add_argument("--headless=new") # Operate Selenium in headless mode
    chrome_options.add_experimental_option('extensionLoadTimeout', 45000) #  Fixes the problem of renderer timeout for a slow PC
    chrome_options.add_argument("--window-size=1920x1080")

    # Step 5: Instantiate a browser object and navigate to the URL
    capabibilties = DesiredCapabilities().CHROME
    capabibilties['pageLoadStrategy'] = 'eager' # Eafer pageLoadStrategy is used to speed up browsing

    # Step 6: Define a function to solve the captcha using the 2captcha service
    def solve_captcha(sitekey, url):
        try:
            result = solver.recaptcha(sitekey=sitekey, url=url)
            captcha_key = result.get('code')
            logging.info(f"Captcha solved. The key is: {captcha_key}\n")
        except Exception as err:
            logging.info(err)
            logging.info(f"Captcha not solved...")
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

    # Step 8: Define a function to invoke the callback function
    def invoke_callback_func(driver, captcha_key):
        try: # Sometimes the captcha is solved without having to invoke the callback function. This piece of code handles this situation
            # html of the captcha is inside an iframe, selenium cannot see it if we first don't switch to the iframe
            WebDriverWait(driver, 15).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "sec-cpt-if")))

            # Inject the token into the inner HTML of g-recaptcha-response and invoke the callback function
            driver.execute_script(f'document.getElementById("g-recaptcha-response").innerHTML="{captcha_key}"')
            driver.execute_script(f"verifyAkReCaptcha('{captcha_key}')") # This step fails in Python but runs successfully in the console
        except TimeoutException:
            logging.info("Captcha was solved without needing to invoke the callback function. Bypassing this part of the script to prevent raising an error")

        # Wait for 0.5 seconds until the page is loaded
        time.sleep(0.5)

    # Step 9: Set the mileage filters
    def mileage_filter_func(driver, km_min, km_max):
        # Wait for the mileage filter to appear
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//select[@id='minMileage-s']")))
        
        # Set the minimum mileage filter
        driver.find_element(by=By.XPATH, value="//select[@id='minMileage-s']").send_keys(km_min)
        time.sleep(1)
        # Set the maximum mileage filter
        driver.find_element(by=By.XPATH, value="//select[@id='maxMileage-s']").send_keys(km_max)
        # Wait for 3 seconds until the the search button retrieves the new number of cars
        time.sleep(3)
        # Click on the search button
        driver.find_element(by=By.XPATH, value="//button[@id='minisearch-search-btn']").click()
        # Wait for 3 seconds until the page re-loads
        time.sleep(3)
    
    # Step 10: Create a function that checks for Captcha and solves it
    def check_for_captcha_and_solve_it_func(driver, try_txt, except_txt):
        # Sometimes, a captcha is shown after navigating to the next page under of a car brand. We need to invoke the captcha service here if that happens
        try:
            # Check for the existence of the "Angebote entsprechen Deinen Suchkriterien" header
            driver.find_element(by=By.XPATH, value="//h1[@data-testid='result-list-headline']").text
            # If the header doesn't exist, proceed normally to the next page
            logging.info(try_txt)
        except NoSuchElementException:
            logging.info(except_txt)
            # If there was a raised exception, this means that the header does not exist, so invoke the solve_captcha function
            captcha_token = solve_captcha(sitekey=sitekey, url=driver.current_url)
            # Invoke the callback function
            invoke_callback_func(driver=driver, captcha_key=captcha_token)

    # Step 11: Define a function to navigate to the filters page and apply the filter
    def nav_and_apply_filters_func(driver, marke, modell):
        # Step 11.2.1: Navigate to the base URL from which we will start our search
        # Declare initial variables before the while loop
        base_url_nav_try_counter = 1
        base_url_nav_is_pass = False

        # Keep trying to navigate to the base URL until it succeeds or 3 tries are exhausted
        while base_url_nav_try_counter <= 3 and base_url_nav_is_pass == False:
            try:
                driver.get(base_url)
                logging.info("\nNavigating to the base URL where we can apply the search criteria...")

                # Step 11.2.2: Maximize the window
                driver.maximize_window()

                # Step 11.2.3: Wait for "Einverstanden" and click on it
                logging.info("Waiting for the Einverstanden window to pop up so we can click on it...")
                try:
                    WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, "//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']")))
                    driver.find_element(by=By.XPATH, value="//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']").click()
                except TimeoutException:
                    logging.info("The Einverstanden/Accept Cookies window did not show up on the apply search criteria page. No need to click on anything...")

                # Step 11.2.4: Apply the filters
                logging.info(f"Applying the search filters for {marke} {modell.strip()}...")
                try:
                    select_marke_modell(driver=driver, marke=marke, modell=modell)
                except NoSuchElementException:
                    logging.info("Marke and Modell chosen not found on the page. Potentially check the spelling of the modell. Stopping the driver, returning an empty list and continuing to the next combination...")
                    # Stop the driver
                    driver.quit()
                    return []
                except ElementClickInterceptedException:
                    logging.info(f"ElementClickInterceptedException error for {marke} {modell.strip()}. Stopping the driver, returning an empty list and continuing to the next combination...")
                    # Stop the driver
                    driver.quit()
                    return []
                base_url_nav_is_pass = True
            except TimeoutException: # This exception catches this error --> Timed out receiving message from renderer
                logging.info("Was not able to navigate to the base URL to apply the filters. Setting base_url_nav_is_pass to False, incrementing base_url_nav_try_counter, and retrying the whole process again starting driver.get(base_url)")
                base_url_nav_is_pass = False
                base_url_nav_try_counter += 1
            
            # If we exhausted the 3 tries and we were still unable to navigate to the base_url, return an empty list and continue to the next combination
            if base_url_nav_try_counter > 3 and base_url_nav_is_pass == False:
                logging.info("There is a problem navigating to the base URL due to the timeout receiving message from renderer error. Stopping the driver, returning an empty list, and continuing to the next combination...")
                # Stop the driver
                driver.quit()
                return []

    # Step 12: Define a function to navigate to the base URL, apply the search filters, bypass the captcha, crawl the data and return it to a JSON file
    def crawl_func(dict_idx):
        # Instantiate the chrome driver
        driver = webdriver.Chrome(desired_capabilities=capabibilties, options=chrome_options)

        # Set page_load_timeout to 45 seconds to avoid renderer timeout
        driver.set_page_load_timeout(45)

        # Step 12.0: Get the marke and modell based on the dict_idx 
        marke = marke_and_modell_list[dict_idx]["marke"]
        modell = marke_and_modell_list[dict_idx]["modell"]

        # Step 12.1: Navigate to the base URL from which we will start our search
        nav_func_var = nav_and_apply_filters_func(driver=driver, marke=marke, modell=modell)
        if nav_func_var == []:
            return []

        # Step 12.5: Solve the captcha
        logging.info(f"Applied the search filters for {marke} {modell.strip()}. Now, solving the captcha...")
        if driver.title == "Challenge Validation":
            captcha_key = solve_captcha(sitekey=sitekey, url=driver.current_url)
            if captcha_key is not None:
                # Declare initial variables before the while loop
                inject_captcha_try_counter = 1
                is_pass = False

                # Keep trying to inject the captcha until it succeeds or 3 tries are exhausted
                while inject_captcha_try_counter <= 3 and is_pass == False:
                    # Invoke the callback function
                    try:
                        invoke_callback_func(driver=driver, captcha_key=captcha_key)
                        is_pass = True
                    # This error could occur because of a problem with setting injecting the g-recaptcha-response in the innerHTML
                    # WebDriverException could happen because the page might crash while trying to switch to the "sec-cpt-if" frame
                    except (JavascriptException, WebDriverException):
                        logging.info("Was not able to inject the g-recaptcha-response in the innerHTML. Setting is_pass to False, incrementing inject_captcha_try_counter, and retrying the whole process again starting from the base URL")
                        is_pass = False
                        inject_captcha_try_counter += 1
                        
                        # Quit the driver
                        driver.quit()
                        
                        # Re-instantiate a new driver
                        driver = webdriver.Chrome(desired_capabilities=capabibilties, options=chrome_options)

                        # Set page_load_timeout to 45 seconds to avoid renderer timeout
                        driver.set_page_load_timeout(45)

                        # Repeat the navigation steps
                        nav_func_var2 = nav_and_apply_filters_func(driver=driver, marke=marke, modell=modell)
                        if nav_func_var2 == []:
                            return []
                    
                    # If we exhausted the 3 tries and the captcha box still did not appear, return an empty list and continue to the next combination
                    if inject_captcha_try_counter > 3 and is_pass == False:
                        logging.info("There is a problem with injecting the g-recaptcha-response in the innerHTML. Stopping the driver, returning an empty list, and continuing to the next combination...")
                        # Stop the driver
                        driver.quit()
                        return []
            else:
                # If the captcha solver did not return a token, return an empty list and proceed to the next marke-modell combination
                logging.info(f"The captcha was not solved for the marke and modell chosen ({marke} {modell.strip()}). Stopping the driver, returning an empty list, and continuing to the next combination...")
                # Stop the driver
                driver.quit()
                return []

        # Step 13: If the captcha token was returned, invoke the callback function and navigate to the results page
        # logging.info the top title of the page
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//h1[@data-testid='result-list-headline']")))
            tot_search_results = re.findall(pattern="\d+", string=driver.find_element(by=By.XPATH, value="//h1[@data-testid='result-list-headline']").text)[0]
            logging.info(f"The results page of {marke} {modell.strip()} has been retrieved. In total, we have {tot_search_results} listings to loop through...")
        except TimeoutException:
            logging.info("The header of the results page does not exist even after waiting for 10 seconds. Stopping the driver, returning an empty list, and continuing to the next combination...")
            # Stop the driver
            driver.quit()
            return []
        except NoSuchElementException: # Another version of the timeout exception where the result-list-headline was not found
            logging.info("The header of the results page was not found due to a 'NoSuchElementException found' error. Stopping the driver, returning an empty list, and continuing to the next combination...")
            # Stop the driver
            driver.quit()
            return []
        except InvalidArgumentException:
            logging.info("The header of the results page was not found due to an 'InvalidArgumentException found' error. Stopping the driver, returning an empty list, and continuing to the next combination...")
            # Stop the driver
            driver.quit()
            return []

        # Create mileage filters for the Porsche brands that have more than 1000 results
        if marke == "Porsche" and modell in ["    911 Urmodell", "    991", "    992", "Boxster", "Cayenne", "Macan", "Panamera", "Taycan"]:
            mileage_filters = [("", "20.000 km"), ("20.000 km", "40.000 km"), ("40.000 km", "60.000 km"), ("60.000 km", "80.000 km"), ("80.000 km", "100.000 km"), ("100.000 km", "150.000 km"), ("150.000 km", "")]
        else:
            mileage_filters = [("", "")]
        
        # Crawl the listing page
        car_page_url_list = []
        for km in mileage_filters:
            # Set the mileage filters
            if km != ("", ""):
                try:
                    mileage_filter_func(driver=driver, km_min=km[0], km_max=km[1])
                except TimeoutException:
                    logging.info("Timeout Exception. Did not find the mileage XPATH selector. Continuing to the next mileage range...")
                    continue

            # Sometimes, a captcha is shown after applying the mileage filters. We need to invoke the captcha service here if that happens
            try:
                check_for_captcha_and_solve_it_func(
                    driver=driver,
                    try_txt=f"No Captcha was found after applying the mileage filters {km[0]} to {km[1]} for {marke} {modell.strip()}. Proceeding normally...",
                    except_txt=f"Captcha found after applying the mileage filters {km[0]} to {km[1]} for {marke} {modell.strip()}. Solving it with the 2captcha service..."
                )
            except JavascriptException:
                logging.info("Javascript Exception: Javascript error. Cannot set properties of null (setting 'innerHTML'). Continuing to the next mileage range...")
                continue

            # Continue on with the rest of the crawling
            # Step 14: We are at the results page now. We need to crawl the links to the individual car pages
            # Step 14.1: Get the landing page URL and last page of the brand-model combination
            landing_page_url = driver.current_url
            last_page_web_element_list = driver.find_elements(by=By.XPATH, value="//span[@class='btn btn--secondary btn--l']")
            try:
                last_page = int(last_page_web_element_list[-1].text)
            except IndexError: # The index error can occur if the brand has only one page. In that case, set last_page to 1
                last_page = 1
            logging.info(f"We have a total of {last_page} pages under {marke} {modell.strip()} to loop through...")

            # Step 14.2: Loop through all the pages of the "marke" and "modell" combination and crawl the individual car links that contain the information we want to crawl
            for pg in range(2, last_page + 2):
                # Step 14.2.1: Get all the car URLs on the page. Don't crawl the "sponsored" or the "top in category" listings 
                logging.info(f"Crawling the car links on page {pg - 1}...")
                try:
                    car_web_elements_selector = "//div[contains(@class, 'cBox-body cBox-body') and @class!='cBox-body cBox-body--topInCategory' and @class!='cBox-body cBox-body--topResultitem']"
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, car_web_elements_selector)))
                    car_web_elements = driver.find_elements(by=By.XPATH, value=car_web_elements_selector)
                    for web in car_web_elements:
                        output_dict_listing_page = {
                            "marke": marke,
                            "modell": modell,
                            "last_page": last_page,
                            "page_rank": pg,
                            "car_page_url": web.find_element(by=By.XPATH, value="./a").get_attribute("href")
                        }
                        
                        car_page_url_list.append(output_dict_listing_page)
                except (InvalidArgumentException, TimeoutException) as err: # Sometimes, the find_elements method produces this error --> Message: invalid argument: uniqueContextId not found
                    logging.info(err)
                
                # Step 14.2.2: Navigate to the next page to collect the next batch of URLs
                if pg <= last_page:
                    logging.info(f"\nMoving to the next page, page {pg}")
                    try:
                        driver.get(landing_page_url + f"&pageNumber={pg}")
                        time.sleep(1)
                        # Sometimes, a captcha is shown after navigating to the next page under of a car brand. We need to invoke the captcha service here if that happens
                        check_for_captcha_and_solve_it_func(
                            driver=driver,
                            try_txt=f"No Captcha was found after navigating to page {pg} under {marke} {modell.strip()}. Proceeding normally...",
                            except_txt=f"Captcha found while navigating to page {pg} under {marke} {modell.strip()}. Solving it with the 2captcha service..."
                        )
                    except TimeoutException:
                        logging.info("TimeoutException: Timed out receiving message from renderer while trying to navigate to the next page. Continuing to the next page...")
                        continue
                    except JavascriptException:
                        logging.info("JavascriptException: Javascript error. Cannot set properties of null (setting 'innerHTML'). Continuing to the next page...")
                        continue
                    except WebDriverException:
                        logging.info("WebDriverException: Session deleted because of page crash. Closing the session and returning the car_page_url_list that was crawled thus far...")
                        break
                else:
                    logging.info(f"Crawled all the car links of {marke} {modell.strip()}...")

        # Stop the driver
        driver.quit()
        return car_page_url_list

    # Step 15: Loop through all the brands in the JSON file
    car_page_url_list = []
    for idx, rec in enumerate(marke_and_modell_list):
        if rec["marke"] not in car_list or rec["modell"] not in modell_list:
            continue
        else:
            car_page_url_list.append(crawl_func(dict_idx=idx))
            # Write the results to a JSON file
            with open(f"car_page_url_list_{category}.json", mode="w", encoding="utf-8") as f:
                json.dump(obj=car_page_url_list, fp=f, ensure_ascii=False, indent=4)
