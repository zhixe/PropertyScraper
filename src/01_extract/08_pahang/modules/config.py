import os, csv, functools
from dotenv import load_dotenv
from datetime import datetime

#  Module
from .extraction import Extraction

class Config:
    def __init__(self):
        self.load_environment_variables()

    @functools.lru_cache(maxsize=None)
    def get_formatted_datetime(self):
        """Return a datetime string formatted for file names."""
        return datetime.now().strftime("%Y%m%d_%H%M%S")  # e.g., '20231230_235959

    @functools.lru_cache(maxsize=None)
    def get_env_path(self, env_var):
        """Retrieve the directory path from the environment variable."""
        return os.path.join(os.getenv("MAIN_DIR"), os.getenv(env_var))

    @property
    def out_dir(self):
        return self.get_env_path("RAW_DIR")

    @property
    def chrome_driver_file(self):
        return os.path.join(self.get_env_path("CONFIG_DIR"), os.getenv("CHROME_DRIVER"))

    @property
    def batch_number(self):
        return os.environ.get('BATCH_NUMBER', '1')

    @property
    def csv_file(self):
        batch_number = self.batch_number
        timestamp = self.get_formatted_datetime()
        script_number = os.getenv('SCRIPT_NUMBER_DECIMAL', 'default_script')
        my_region = os.getenv('MY_REGION')
        return os.path.join(self.out_dir, f"batch{batch_number}_{script_number}_{my_region}_iproperty_{timestamp}.csv")

    # @property
    # def excel_file(self):
    #     batch_number = self.batch_number
    #     script_number = os.getenv('SCRIPT_NUMBER_DECIMAL', 'default_script')
    #     my_region = os.getenv('MY_REGION')
    #     return os.path.join(self.out_dir, f"batch{batch_number}_{script_number}_{my_region}_iproperty.xlsx")

    @property
    def log_dir(self):
        return self.get_env_path("LOG_DIR")

    @property
    def web_url(self):
        script_number = os.getenv('SCRIPT_NUMBER')
        return os.getenv(f"WEBURL{script_number}")

    @property
    def headers(self):
        # Assuming Extraction class has a way to provide headers
        scraper = Extraction(self)
        return list(scraper.extractors.keys())

    @functools.cached_property
    def csv_file_path(self):
        batch_number = self.batch_number
        timestamp = self.get_formatted_datetime()
        script_number = os.getenv('SCRIPT_NUMBER_DECIMAL', 'default_script')
        my_region = os.getenv('MY_REGION')
        return os.path.join(self.out_dir, f"batch{batch_number}_{script_number}_{my_region}_iproperty_{timestamp}.csv")

    # @functools.cached_property
    # def excel_file_path(self):
    #     batch_number = self.batch_number
    #     script_number = os.getenv('SCRIPT_NUMBER_DECIMAL', 'default_script')
    #     my_region = os.getenv('MY_REGION')
    #     return os.path.join(self.out_dir, f"batch{batch_number}_{script_number}_{my_region}_iproperty.xlsx")

    @functools.cached_property
    def json_file_path(self):
        return os.path.join(self.out_dir, os.getenv('SCHEMA_DIR'))

    # def remove_existing_csv(self):
    #     if os.path.exists(self.csv_file):
    #         os.remove(self.csv_file)

    # def remove_existing_excel(self):
    #     if os.path.exists(self.excel_file):
    #         os.remove(self.excel_file)

    def load_environment_variables(self):
        dotenv_path = os.path.join(os.getcwd(), '../../.env')
        load_dotenv(dotenv_path)
        # self.remove_existing_csv()

    def create_folders(self):
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(self.out_dir, exist_ok=True)
        
        # Initialize the Extraction class
        scraper = Extraction(self)
        headers = scraper.extractors.keys()
        self.create_empty_csv_file(headers)
        # self.create_empty_excel_file(headers)

    def create_empty_csv_file(self, headers):
        with open(self.csv_file_path, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(headers)

    # def create_empty_excel_file(self, headers, file_name=None):
    #     wb = Workbook()
    #     ws = wb.active
    #     ws.title = 'iproperty'  # Set the worksheet title
    #     ws.append(list(headers))  # Add the headers to the first row

    #     if file_name is None:
    #         excel_file_path = os.path.join(self.out_dir, self.excel_file_path)
    #     else:
    #         excel_file_path = os.path.join(self.out_dir, f"{file_name}.xlsx")

    #     wb.save(excel_file_path)

    # def create_excel_files_from_json(self):
    #     with open(self.json_file_path, 'r') as file:
    #         data = json.load(file)

    #     for key, script_path in data['Script'].items():
    #         file_name = os.path.splitext(os.path.basename(script_path))[0]
    #         self.create_empty_excel_file(self.headers, file_name)