import pandas as pd, re, os, glob, functools, json, sys
from datetime import datetime
from dotenv import load_dotenv

class Config:
    def __init__(self):
        self.load_environment_variables()

    @functools.lru_cache(maxsize=None)
    def _get_env_path(self, env_var):
        return os.path.join(os.getenv("MAIN_DIR"), os.getenv(env_var))

    def load_environment_variables(self):
        dotenv_path = os.path.join(os.getcwd(), '../../.env')
        load_dotenv(dotenv_path)

    @property
    def out_dir(self):
        return self._get_env_path("RAW_DIR")

    @property
    def staging_dir(self):
        return self._get_env_path("STAGING_DIR")

    @property
    def schema_dir(self):
        return self._get_env_path("SCHEMA_DIR")

    def create_folders(self):
        os.makedirs(self.staging_dir, exist_ok=True)

    @functools.cached_property
    def csv_files(self):
        csv_file_pattern = os.path.join(self.out_dir, '*.csv')
        return glob.glob(csv_file_pattern)



class DataCleaner:
    @staticmethod
    def clean_square_footage(sq_ft):
        if pd.isna(sq_ft):
            return pd.NA

        if isinstance(sq_ft, str):
            sq_ft = sq_ft.replace('from ', '')

            if ' - ' in sq_ft:
                sq_ft = sq_ft.split(' - ')[0]

            sq_ft = re.sub(r'[^\d.]+', '', sq_ft)

            try:
                return float(sq_ft)
            except ValueError:
                return pd.NA
        else:
            try:
                return float(sq_ft)
            except (ValueError, TypeError):
                return pd.NA

    @staticmethod
    def clean_posted_date(date_str):
        if not isinstance(date_str, str) or not date_str.strip():
            return None  # return None if date_str is not valid

        now = datetime.now()

        if 'today' in date_str:
            if time_str := re.search(r'(\d{2}:\d{2} [ap]m)', date_str):
                time_str = time_str[1]
                return datetime.strptime(f"{now.strftime('%Y-%m-%d')} {time_str}", '%Y-%m-%d %I:%M %p')
        elif 'yesterday' in date_str:
            yesterday = now - pd.Timedelta(days=1)
            if time_str := re.search(r'(\d{2}:\d{2} [ap]m)', date_str):
                time_str = time_str[1]
            else:
                time_str = '12:00 am'  # default to midnight if no time is found
            return datetime.strptime(f"{yesterday.strftime('%Y-%m-%d')} {time_str}", '%Y-%m-%d %I:%M %p')
        else:
            date_part = re.sub(r'posted on ', '', date_str).strip()
            try:
                return datetime.strptime(date_part, '%d %b %Y %I:%M %p')
            except ValueError:
                try:
                    return datetime.strptime(date_part, '%d %b %Y')
                except ValueError:
                    return None  # return None if date_str is not valid
        return None  # return None if date_str is not valid




    @staticmethod
    def clean_and_capitalize(text):
        if not isinstance(text, str):
            return text
        return ' '.join(word.capitalize() for word in re.sub(r'\s{2,}|\t', ' ', text).replace('-', ' ').strip().split())

    @staticmethod
    def replace_start_nan(x):
        return '' if isinstance(x, str) and x[:3].lower() == 'nan' else x

    @staticmethod
    def calculate_mid_value(price_range_str):
        if '-' in price_range_str:
            num1, num2 = map(float, price_range_str.split('-'))
            return (num1 + num2) / 2
        elif price_range_str.isdigit():
            return float(price_range_str)
        else:
            return None  # or some other value that indicates a non-numeric string

    @staticmethod
    def check_land(row):
        if row['House_Type'] in ['Residential Land']:
            return row['House_Price']
        else:
            return DataCleaner.three_comma_filter(str(row['House_Price']))

    @staticmethod
    def three_comma_filter(input_str):
        # Check if input_str has less than two commas
        if input_str.count(',') < 3:
            return input_str

        # Check if input_str has equal or less than 9 digits
        digits = [char for char in input_str if char.isdigit()]
        if len(digits) < 9:
            return input_str

        # Check if the last three digits are equal to 0
        if int(''.join(digits[-3:])) != 0:
            return input_str

        # If all conditions are met, remove three last digits
        return input_str[:-4]


    @staticmethod
    def is_land_or_bungalow1(row):
        if row['House_Type'] in ['Residential Land', 'Bungalow']:
            return row['House_Price']
        else:
            return DataCleaner.remove_digits(row['House_Price'])

    @staticmethod
    def remove_digits(input_str):
        # Check if input_str has two commas
        if input_str.count(',') != 2:
            return input_str

        # Check if input_str has 9 digits
        digits = [char for char in input_str if char.isdigit()]
        if len(digits) != 9:
            return input_str

        # Check if the last three digits are greater than 0
        if int(''.join(digits[-3:])) == 0:
            return input_str

        # If all conditions are met, remove 2 digits in the middle
        return input_str.replace(',000', ',0', 1)

    @staticmethod
    def is_land_or_bungalow2(row):
        if row['House_Type'] in ['Residential Land', 'Bungalow']:
            return row['House_Price']
        else:
            return DataCleaner.nine_digits_three_zero_trail_filter(str(row['House_Price']))

    @staticmethod
    def nine_digits_three_zero_trail_filter(input_str):
        # Check if input_str has two commas
        if input_str.count(',') != 2:
            return input_str

        # Check if input_str has 9 digits
        digits = [char for char in input_str if char.isdigit()]
        if len(digits) != 9:
            return input_str

        # Check if the last three digits are equal to 0
        if int(''.join(digits[-3:])) != 0:
            return input_str

        # If all conditions are met, remove three last digits
        return input_str[:-4]



class SchemaHandler:
    @staticmethod
    def read_schema(schema_file_path):
        with open(schema_file_path, 'r') as file:
            return json.load(file)

    @staticmethod
    def handle_numerical_nan(df, schema):
        for column, data_type in schema.items():
            if "DECIMAL" in data_type or "INTEGER" in data_type:
                adjusted_column = column.strip('[]')
                if adjusted_column in df.columns:
                    # df[adjusted_column] = df[adjusted_column].apply(lambda x: 0 if pd.isna(x) else x)
                    # Uncomment next line if you prefer NULL values instead of 0
                    df[adjusted_column] = df[adjusted_column].apply(lambda x: None if pd.isna(x) else x)
        return df



class DataTransformer:
    def __init__(self, schema):
        self.schema = schema

    def transform_data(self, df, file_name):
        df = df.copy()
        df = df[~df['House_Price'].str.contains('contact', case=False, na=False)] # Remove all rows with 'contact for detail' in House_Price
        df.loc[:, 'Property_ID'] = df['Page_Link'].str.extract(r'([^\/]+)\/?$').astype('str')
        df.loc[:, 'Area'] = df['Page_Link'].str.extract(r'/property/([^\/]+)/').astype('str')
        df.loc[:, 'Square_Footage'] = df['Square_Footage'].apply(DataCleaner.clean_square_footage)
        df.loc[:, 'Posted_Date'] = pd.to_datetime(df['Posted_Date'].apply(DataCleaner.clean_posted_date))
        df.loc[:, 'Posted_Date'] = df.dropna(subset=['Posted_Date'])

        # df['House_Price'] = df['House_Price'].str.extract(r'rm (\d+,\d+|\d+)')
        df.loc[:, 'House_Price'] = df['House_Price'].replace(r'rm ', '', regex=True)
        df.loc[:, 'House_Price'] = df['House_Price'].replace(r'from ', '', regex=True)
        df.loc[:, 'House_Price'] = df.apply(DataCleaner.is_land_or_bungalow1, axis=1)
        df.loc[:, 'House_Price'] = df.apply(DataCleaner.is_land_or_bungalow2, axis=1)
        df.loc[:, 'House_Price'] = df.apply(DataCleaner.check_land, axis=1)
        df.loc[:, 'House_Price'] = df['House_Price'].replace(r',', '', regex=True)
        df.loc[:, 'House_Price'] = df['House_Price'].apply(DataCleaner.calculate_mid_value)
        df.loc[:, 'House_Price'] = df['House_Price'].astype('float')
        df = df[df['House_Price'] >= 25000] # Remove all rows with price below 25000 due to fake sales price

        # df['Price_Square_Feet'] = df['Price_Square_Feet'].str.extract(r'rm (\d+\.\d+)')
        df.loc[:, 'Price_Square_Feet'] = df['Price_Square_Feet'].replace(r'rm ', '', regex=True)
        df.loc[:, 'Price_Square_Feet'] = df['Price_Square_Feet'].replace(r',', '', regex=True) 
        df.loc[:, 'Price_Square_Feet'] = df['Price_Square_Feet'].astype('float')

        df.loc[:, 'House_Price'] = df['House_Price'].fillna(0)
        df.loc[:, 'Price_Square_Feet'] = df['Price_Square_Feet'].fillna(0)
        df.loc[:, 'Square_Footage'] = df['Square_Footage'].fillna(0)

        columns_to_clean = ['Agent_Name', 'House_Name', 'House_Location', 'House_Type', 'Lot_Type', 'House_Furniture', 'Area']
        for col in columns_to_clean:
            df[col] = df[col].apply(DataCleaner.clean_and_capitalize).astype('str')

        replacements = {
            r'\bHomes\b': 'House',
            r'\bSty\b': 'Storey',
            r'\blink\b': 'Link'
        }
        columns_to_clean2 = ['House_Type']
        # Apply the replacements to each column
        for col in columns_to_clean2:
            df[col] = df[col].replace(replacements, regex=True)

        state = ' '.join(re.findall(r'batch\d+_\d+_([^_]+)_iproperty_\d+_\d+.csv', file_name)[0].replace('-', ' ').split()).title()
        df.insert(loc=3, column='State', value=state)
        return df



class DataProcessor:
    def __init__(self, config):
        self.config = config
        self.schema = SchemaHandler.read_schema(os.path.join(config.schema_dir, 'mssql_iproperty.json'))
        self.transformer = DataTransformer(self.schema)

    def process_files(self):
        transformed_data = []
        for csv_file in self.config.csv_files:
            try:
                df = pd.read_csv(csv_file)
                if df.empty:
                    print(f"Skipping empty file: {csv_file}")
                    continue
                df = df[df['Page_Link'].notna() & (df['Page_Link'] != '')]
                file_name = os.path.basename(csv_file)
                transformed_df = self.transformer.transform_data(df, file_name)
                transformed_df = transformed_df.map(DataCleaner.replace_start_nan)
                transformed_df = SchemaHandler.handle_numerical_nan(transformed_df, self.schema)
                transformed_df = self.reorganize_columns(transformed_df)  # Reorganize columns
                transformed_data.append(transformed_df)
            except pd.errors.EmptyDataError:
                print(f"No data to parse in file: {csv_file}")
            except Exception as e:
                print(f"Error processing file {csv_file}: {e}")
        return pd.concat(transformed_data) if transformed_data else pd.DataFrame()

    def reorganize_columns(self, df):
        # Define the new order of columns
        new_order = ['Property_ID', 'Page_Link', 'Source', 'Agent_Name', 'State', 'Area', 'House_Price', 'Price_Square_Feet', 'House_Name', 'House_Location', 'House_Type', 'Lot_Type', 'Square_Footage', 'House_Furniture', 'Posted_Date', 'Created_At']

        # Reorganize columns and handle any missing columns
        df = df.reindex(columns=new_order, fill_value=None)
        return df

    def save_transformed_data(self):
        data = self.process_files()
        if not data.empty:
            staging_file = os.path.join(self.config.staging_dir, 'staging_data.csv')
            data.to_csv(staging_file, index=False)
        else:
            print("No data was processed. Check the input files.")



# Main
config = Config()
config.create_folders()
processor = DataProcessor(config)
processor.save_transformed_data()
sys.exit(0)