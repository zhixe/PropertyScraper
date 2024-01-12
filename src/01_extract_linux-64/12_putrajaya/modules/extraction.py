
import os, re, time, datetime, pandas as pd, functools
from dateutil.parser import parse
from tqdm import tqdm
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService

# Modules
from .mapping import Mapping
from .setup import Setup
from .save import OutputHandler

class Extraction:
    def __init__(self, config):
        #  Global scope
        self.data = []
        self.visited_urls = set()

        # Import parameters from config.py
        self.config = config
        self.url = config.web_url
        self.driver = config.chrome_driver_file

        # Import parameters from save.py
        self.output_handler = OutputHandler(config)

        # Import parameters from local methods
        self.extractors = self.initialize_extractors()

    def initialize_extractors(self):
        return {
            "Page_Link": Mapping.extract_page_link,
            "Source": Mapping.extract_source,
            "Agent_Name": lambda listing: Mapping.extract_element_text(listing, ".//div[contains(@class,'ListingHeadingstyle__HeadingTitle')] | .//div[contains(@class,'heading-name')]"),
            "Posted_Date": lambda listing: Mapping.extract_element_text(listing, ".//p[contains(@class,'ListingHeadingstyle__HeadingCreationDate')] | .//div[contains(@class,'BasicCardstyle__HeadingWrapper-DxbUP knxJEk')]/p[contains(@class,'heading-creation-date')]"),
            "House_Price": lambda listing: Mapping.extract_element_text(listing, ".//li[contains(@class,'ListingPricestyle__ItemWrapper')] | .//div[contains(@class,'ListingPricestyle__RangePriceWrapper')]"),
            "Price_Square_Feet": Mapping.extract_price_square_feet,
            "House_Name": lambda listing: Mapping.extract_element_text(listing, ".//h2[contains(@class,'PremiumCardstyle__TitleWrapper')] | .//div[contains(@class,'BasicCardstyle__DescriptionWrapper-dPdKmp fRMZJr')]//h2[contains(@class,'BasicCardstyle__TitleWrapper-eNIiIX dLdNwJ')]"),
            "House_Location": lambda listing: Mapping.extract_element_text(listing, ".//div[contains(@class,'PremiumCardstyle__AddressWrapper')] | .//div[contains(@class,'BasicCardstyle__DescriptionWrapper-dPdKmp fRMZJr')]//div[contains(@class,'BasicCardstyle__AddressWrapper-jUpzVZ jikSUL')]"),
            "House_Type": Mapping.extract_house_type,
            "Lot_Type": Mapping.extract_lot_type,
            "Square_Footage": Mapping.extract_square_footage,
            "House_Furniture": Mapping.extract_house_furniture,
            "Created_At": Mapping.get_current_datetime
        }

    def setup_driver_and_browser(self):
        chrome_options = Setup.setup_driver()
        self.driver = Setup.instantiate_browser(self.config.chrome_driver_file, chrome_options)
        self.driver.get(self.url)
        time.sleep(10)
        Setup.wait_for_element(self.driver, 5, "//ul[contains(@class,'ListingsListstyle__ListingsListContainer')]")

    def instantiate_browser(self, chrome_driver_file, chrome_options):
        service = ChromeService(chrome_driver_file)
        web_driver = webdriver.Chrome(service=service, options=chrome_options)
        Setup.configure_stealth(web_driver)

    @property
    def find_listings(self):
        return self.driver.find_elements(By.XPATH, "//li[contains(@class,'ListingsListstyle__ListingListItemWrapper')]")

    def extract_data_from_listing(self, listing):
        return {
            key: self._extract_data_value(value, listing)
            for key, value in self.extractors.items()
        }

    def _extract_data_value(self, value, listing):
        return self._extract_callable_data(value, listing) if callable(value) else Mapping.extract_element_text(listing, value)

    def _extract_callable_data(self, value, listing):
        return value(listing) if self._function_expects_argument(value) else value()

    def _function_expects_argument(self, func):
        return func.__code__.co_argcount > 0

    def scrape_page(self):
        current_url = self.driver.current_url
        if current_url in self.visited_urls:
            return
        self.visited_urls.add(current_url)
        listings = self.find_listings
        self._process_listings(listings)

    def _process_listings(self, listings):
        for listing in listings:
            try:
                data = self.extract_data_from_listing(listing)
                self.data.append(data)
            except Exception as e:
                print(f"Error processing listing: {e}")
        self.save_and_clear_data()

    def save_and_clear_data(self):
        output_csv_file = self.initialize_output_csv()
        # output_excel_file = self.initialize_output_excel()

        # Check and fill 'Posted Date' if necessary
        for item in self.data:
            item.setdefault('Posted Date', None)  # Ensure 'Posted Date' is present

        # Now, save the data to files
        self.save_data_to_csv(output_csv_file)
        # self.save_data_to_excel(output_excel_file)
        self.data.clear()  # Clear the data after saving

    def wait_for_next_page_button(self):
        retries = 0
        while retries < 3:
            try:
                return self.driver.find_element(
                    By.XPATH,
                    "//li[contains(@class,'pagination-item')]/a[contains(@aria-label,'Go to next page')]",
                )
            except NoSuchElementException:
                retries += 1
                print("\nWaiting for Next Page button to become available...")
                time.sleep(1)
                # self.driver.refresh()
        return None

    def scrape_all_pages(self, progress_bar):
        while self.scrape_next_page(progress_bar):
            pass

    def scrape_next_page(self, progress_bar):
        self.scrape_page()
        progress_bar.update(1)

        if self.check_browser_message():
            self.handle_browser_message()

        if not self.has_next_page():
            print("Scraping finished.")
            return False

        self.scroll_to_bottom()  # Scroll to the bottom before clicking the next page button
        self.click_next_page_button()
        time.sleep(5)

        # After navigating to the next page, scrape data from the new page
        self.scrape_page()
        return True

    def check_browser_message(self):
        return "Checking your browser" in self.driver.page_source

    def handle_browser_message(self):
        time.sleep(5)
        self.driver.find_element_by_id("button").click()

    def scroll_to_bottom(self):
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(5)  # Wait for a few seconds to allow any new content to load

    def has_next_page(self):
        next_page_button = self.wait_for_next_page_button()
        return next_page_button is not None and 'disabled' not in next_page_button.get_attribute('class')

    def click_next_page_button(self):
        if next_page_button := self.wait_for_next_page_button():
            next_page_button.click()

    def perform_scraping(self):
        progress_bar = self.initialize_scraping_process()
        self.scrape_all_pages(progress_bar)

    def initialize_scraping_process(self):
        self.setup_driver_and_browser()
        total_pages = len(self.driver.find_elements(By.XPATH, "//li[contains(@class,'pagination-item')]"))
        return tqdm(total=total_pages, desc="Scraping Progress", unit="page")

    def initialize_output_csv(self):
        return self.config.csv_file

    # def initialize_output_excel(self):
    #     return self.config.excel_file

    def process_dataframe(self, df):
        df = df.apply(lambda x: x.strip().lower() if isinstance(x, str) else x)
        df.replace('', 'NULL', inplace=True)
        return df

    def save_data_to_csv(self, output_csv_file):
        try:
            # Create a DataFrame from self.data
            df = pd.DataFrame(self.data, columns=self.extractors.keys())
            df = self.process_dataframe(df)

            # Load existing data
            if os.path.exists(output_csv_file):
                df_existing = pd.read_csv(output_csv_file)
                # Drop columns in df_existing that are all NA before concatenation
                df_existing = df_existing.dropna(axis=1, how='all')
                df.dropna(axis=1, how='all', inplace=True)
                df = pd.concat([df_existing, df])

            # Drop duplicates
            df = df.map(lambda x: x.strip().lower() if isinstance(x, str) else x) # Remove the first strip of whitespace and chg strings to lowercase
            df.drop_duplicates(subset=self.extractors.keys(), inplace=True, ignore_index=True)

            # Save the DataFrame to CSV
            df.to_csv(output_csv_file, header=True, index=False)
        except Exception as e:
            print(f"Error during save: {e}")

    # def save_data_to_excel(self, output_excel_file):
    #     # Create a DataFrame from self.data
    #     df_new = pd.DataFrame(self.data)
    #     # Process the DataFrame (convert dates, calculate averages, etc.)
    #     df_new = self.process_dataframe(df_new)
    #     # Drop duplicates based on all columns
    #     df_new.drop_duplicates(inplace=True)

    #     # Check if the Excel file already exists
    #     if os.path.exists(output_excel_file):
    #         # Read the existing Excel file into a DataFrame
    #         df_existing = pd.read_excel(output_excel_file)
    #         # Concatenate the old and new data
    #         df_combined = pd.concat([df_existing.dropna(axis=1, how='all'), df_new.dropna(axis=1, how='all')], ignore_index=True)
    #         # Explicitly handle all-NA columns
    #         df_combined = df_combined.dropna(axis=1, how='all')
    #         # Drop duplicates again after concatenation
    #         df_combined.drop_duplicates(inplace=True)

    #         # Use xlsxwriter to save the combined DataFrame
    #         with pd.ExcelWriter(output_excel_file, engine='xlsxwriter') as writer:
    #             df_combined.to_excel(writer, index=False, sheet_name='iproperty')
    #     else:
    #         # If the Excel file doesn't exist, write the new data to it
    #         with pd.ExcelWriter(output_excel_file, engine='xlsxwriter') as writer:
    #             df_new.to_excel(writer, index=False, sheet_name='iproperty')

    def initialize_and_save_output_files(self):
        output_csv_file = self.initialize_output_csv()
        # output_excel_file = self.initialize_output_excel()
        self.save_data_to_csv(output_csv_file) # Save to CSV
        # self.save_data_to_excel(output_excel_file)  # Save to Excel

    def scrape_and_save_data(self):
        try:
            self.perform_scraping()
            self.initialize_and_save_output_files()

        except Exception as e:
            self.log_exception(e)

        finally:
            if self.driver:
                self.driver.quit()

    def log_exception(self, e):
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        log_file_name = fr"{self.config.log_dir}/error_log_{timestamp}.log"
        with open(log_file_name, "w") as log_file:
            log_file.write(error_message + "\n")