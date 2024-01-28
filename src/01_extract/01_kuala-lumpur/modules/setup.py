from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium_stealth import stealth
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class Setup:
    chrome_driver_file = None

    def __init__(self, chrome_driver_file):
        self.chrome_driver_file = chrome_driver_file
        chrome_options = Setup.setup_driver()
        self.driver = Setup.instantiate_browser(self.chrome_driver_file, chrome_options)

    @staticmethod
    def setup_driver():
        options = ChromeOptions()
        options.add_argument('--headless')  # Enable headless mode
        options.add_argument('--disable-gpu')  # This option is necessary for headless mode
        options.add_argument('--no-sandbox')  # This option is often necessary if you run under a UNIX system
        options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        # stealth(options)
        return options

    @staticmethod
    def instantiate_browser(chrome_driver_file, chrome_options):
        service = ChromeService(chrome_driver_file)
        web_driver = webdriver.Chrome(service=service, options=chrome_options)
        Setup.configure_stealth(web_driver)
        return web_driver

    @staticmethod
    def wait_for_element(driver, timeout, xpath):
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )

    @staticmethod
    def configure_stealth(driver):
        stealth(driver,
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.53 Safari/537.36',
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
            run_on_insecure_origins=False,
            window_name="my_custom_window"
        )
