# Import packages
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Helper functions
def change_cwd():
    if any([True if i in os.getcwd() else False for i in [
        # Windows
        "mobile_de_scraper\mobile_de", "lukas_mobile_de_crawling\mobile_de",
        # Mac and Linux
        "mobile_de_scraper/mobile_de", "lukas_mobile_de_crawling/mobile_de"
    ]]):
        pass
    else:
        os.chdir(os.getcwd() + "/mobile_de")

# Custom scrapy settings
custom_scrapy_settings = {
    "FEED_EXPORT_ENCODING": "utf-8", # UTF-8 deals with all types of characters
    "RETRY_TIMES": 3, # Retry failed requests up to 3 times
    "AUTOTHROTTLE_ENABLED": False, # Disables the AutoThrottle extension (recommended to be used if you are not using proxy services)
    "RANDOMIZE_DOWNLOAD_DELAY": False, # Should not be used with proxy services. If enabled, Scrapy will wait a random amount of time (between 0.5 * DOWNLOAD_DELAY and 1.5 * DOWNLOAD_DELAY) while fetching requests from the same website
    "CONCURRENT_REQUESTS": 10, # The maximum number of concurrent (i.e. simultaneous) requests that will be performed by the Scrapy downloader
    "DOWNLOAD_TIMEOUT": 60, # Setting the timeout parameter to 60 seconds as per the ScraperAPI documentation
    "ROBOTSTXT_OBEY": False, # Don't obey the Robots.txt rules
    "LOG_LEVEL": "DEBUG", # Set the level of logging to DEBUG
    # Zyte settings
    "DOWNLOAD_HANDLERS": {
        "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
    },
    "DOWNLOADER_MIDDLEWARES": {
        "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000,
    },
    "REQUEST_FINGERPRINTER_CLASS": "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
    "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    "ZYTE_API_KEY": os.getenv("ZYTE_API_KEY"),
    "ZYTE_API_LOG_REQUESTS": True,
    "ZYTE_API_TRANSPARENT_MODE": True,
    "ZYTE_API_SKIP_HEADERS": ["Cookie", "User-Agent"],
    "ZYTE_API_RETRY_POLICY": "retry_policies.CUSTOM_RETRY_POLICY"
}

# General global inputs
listing_page_crawling_framework = "zyte" # Set the listing page crawling framework to be used (options: "selenium" or "zyte")
marke_list = [
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
]
modell_list = [
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
    "Mulsanne",
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
    "750S",
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