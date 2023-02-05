from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import pyautogui
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Set chrome options
chrome_options = Options()
chrome_options.add_argument('start-maximized') # Required for a maximized Viewport
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
chrome_options.add_experimental_option("detach", True)
chrome_options.add_experimental_option('prefs', {'intl.accept_languages': 'en,en_US'})

# Instantiate a browser object and navigate to the URL
driver = webdriver.Chrome(chrome_options=chrome_options)

url = "https://suchen.mobile.de/fahrzeuge/search.html?dam=0&isSearchRequest=true&ms=8600%3B51%3B%3B&ref=quickSearch&sb=rel&vc=Car"
driver.get(url)
driver.maximize_window()

# html of the captcha is inside an iframe, selenium cannot see it if we first don't switch to the iframe
WebDriverWait(driver, 9).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "sec-cpt-if")))

# wait until the captcha is visible on the screen
WebDriverWait(driver, 9).until(EC.visibility_of_element_located((By.CSS_SELECTOR, '#g-recaptcha')))

# find captcha on page
checkbox = pyautogui.locateOnScreen('box.png')
if checkbox:
    # compute the coordinates (x,y) of the center
    center_coords = pyautogui.center(checkbox)
    center_coords = pyautogui.moveTo(center_coords [0], center_coords [1], duration = 0.5)
    pyautogui.click()
else:
    print('Captcha not found on screen')