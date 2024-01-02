import os

# Modules
from modules.config import Config
from modules.extraction import Extraction

class Main:
    def __init__(self):
        self.config = Config()

    def create_empty_csv_file(self):
        self.config.create_empty_csv_file(Extraction(self.config).extractors.keys())

    # def create_empty_excel_file(self):
    #     # Convert the headers to a list and then create the Excel file
    #     headers = list(Extraction(self.config).extractors.keys())
    #     self.config.create_empty_excel_file(headers)

    def initiate_scraper(self):
        self.scraper = Extraction(self.config)
        self.scraper.setup_driver_and_browser()

    def run_scraping_task(self):
        self.scraper.scrape_and_save_data()

    def clear_environment(self):
        os.environ.clear()

    def execute(self):
        self.config.create_folders()
        self.create_empty_csv_file()
        # self.create_empty_excel_file()
        self.initiate_scraper()
        self.run_scraping_task()
        self.clear_environment()

if __name__ == "__main__":
    executor = Main()
    executor.execute()