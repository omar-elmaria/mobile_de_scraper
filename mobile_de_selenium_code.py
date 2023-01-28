from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from twocaptcha import TwoCaptcha
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
import os
import time

# Load environment variables
load_dotenv()

# Instantiate a solver object
solver = TwoCaptcha(os.getenv("CAPTCHA_API_KEY"))
sitekey = "6Lfwdy4UAAAAAGDE3YfNHIT98j8R1BW1yIn7j8Ka"
url = "https://suchen.mobile.de/fahrzeuge/search.html?dam=0&isSearchRequest=true&ms=8600%3B51%3B%3B&ref=quickSearch&sb=rel&vc=Car"

# Set chrome options
chrome_options = Options()
chrome_options.add_argument('start-maximized') # Required for a maximized Viewport
chrome_options.add_argument("--headless=chrome")
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument("--window-size=1920x1080")
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
chrome_options.add_experimental_option("detach", True)
chrome_options.add_experimental_option('prefs', {'intl.accept_languages': 'en,en_US'})

# Instantiate a browser object and navigate to the URL
driver = webdriver.Chrome(chrome_options=chrome_options)

# Navigate to the URL
driver.get(url)

# Minimize the window
driver.minimize_window()

# Solve the captcha using the 2captcha service
def solve(sitekey, url):
    try:
        result = solver.recaptcha(sitekey=sitekey, url=url)
    except Exception as e:
        exit(e)

    return result.get('code')

captcha_key = solve(sitekey=sitekey, url=url)
print(captcha_key)

# html of the captcha is inside an iframe, selenium cannot see it if we first don't switch to the iframe
WebDriverWait(driver, 9).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "sec-cpt-if")))

# Inject the token into the inner HTML of g-recaptcha-response and invoke the callback function
driver.execute_script(f'document.getElementById("g-recaptcha-response").innerHTML="{captcha_key}"')
driver.execute_script(f"verifyAkReCaptcha('{captcha_key}')") # This step fails in Python but runs successfully in the console

# Wait for 3 seconds until the "Accept Cookies" window appears. Can also do that with WebDriverWait.until(EC)
time.sleep(3)

# Click on "Einverstanden"
driver.find_element(by=By.XPATH, value="//button[@class='sc-bczRLJ iBneUr mde-consent-accept-btn']").click()

# Wait for 0.5 seconds until the page is loaded
time.sleep(0.5)

# Print the top title of the page
print(driver.find_element(by=By.XPATH, value="//h1[@data-testid='result-list-headline']").text)