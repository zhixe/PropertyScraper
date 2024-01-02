import os, csv, json, logging, functools, psycopg2, hashlib, sys
from sqlalchemy import create_engine, text
from functools import cached_property
from dotenv import load_dotenv
from datetime import datetime
from psycopg2 import sql

class Config:
    def __init__(self): # Loading environment variables during class instantiation
        self.load_environment_variables()

    @functools.lru_cache(maxsize=None)
    def _get_env_path(self, env_var): # Caching the path retrieval to avoid redundant computations for repeated calls
        return os.path.join(os.getenv("MAIN_DIR"), os.getenv(env_var))

    def load_environment_variables(self): # Load environment variables from a .env file located two directories above the current working directory
    # Construct a dotenv directory path using a provided environment variable key
        dotenv_path = os.path.join(os.getcwd(), '../../.env')
        load_dotenv(dotenv_path)

    @property
    def key_name(self): # Property for retrieving the KEY_SCHEMA_STAGING key name from environment variables
        return os.getenv("KEY_SCHEMA_STAGING")  # Fetch the environment variable directly

    @property
    def out_dir(self): # Property for retrieving the OUTPUT directory name from environment variables
        return self._get_env_path("STAGING_DIR")

    @property
    def log_dir(self): # Property for retrieving the LOG directory name from environment variables
        return self._get_env_path("LOG_DIR")

    @property
    def schemadir(self): # Property for retrieving the SCHEMA directory name from environment variables
        return self._get_env_path("SCHEMA_DIR")

    def create_folders(self): # Create the log directory if not existed
        os.makedirs(self.log_dir, exist_ok=True)

    def _get_schema_path(self, script_basename_without_ext): # Construct the schema file path using the schema directory and a provided script basename
        return os.path.join(self.schemadir, f"{script_basename_without_ext}.json")

    @functools.lru_cache(maxsize=None)
    def get_schema_data(self, script_basename_without_ext): # Caching the schema data retrieval to avoid redundant file reads for repeated calls
    # Retrieve schema data from a JSON file located in the schema directory
        with open(self._get_schema_path(script_basename_without_ext), 'r') as f:
            return json.load(f)

    @staticmethod
    def get_dataset_name(schema_data):
        return next(
            (
                dataset_name
                for dataset_name in schema_data.keys()
                if dataset_name == "staging_iproperty"
            ),
            None,
        )



class Logger:
    def __init__(self, dataset_name, log_dir):
        self.dataset_name = dataset_name
        self.log_dir = log_dir
        self.log_file = self.get_log_filename()
        self.setup_logging()

    @functools.lru_cache(maxsize=1)
    def get_log_filename(self):
        filename = f"{self.log_dir}/log_PROD_{self.dataset_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        return filename

    @functools.cached_property
    def console_handler(self):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(console_formatter)
        return console_handler

    def setup_basic_config(self):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename=self.get_log_filename())

    def setup_console_handler(self):
        root_logger = logging.getLogger()
        if not any(isinstance(handler, logging.StreamHandler) for handler in root_logger.handlers):
            root_logger.addHandler(self.console_handler)

    def setup_logging(self):
        # Set the root logger level
        logging.basicConfig(level=logging.INFO)

        # Create a file handler
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(file_formatter)
        logging.getLogger().addHandler(file_handler)

        # Create a console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(console_formatter)
        logging.getLogger().addHandler(console_handler)

    def get_logger(self):
        return logging.getLogger()



class Database:
    def __init__(self): # Initialize database connection parameters from environment variables
        self.pg_host = os.getenv("pgsqlHost")
        self.pg_port = os.getenv("pgsqlPort")
        self.pg_username = os.getenv("pgsqlUsername")
        self.pg_password = os.getenv("pgsqlPassword")
        self.pg_database = os.getenv("pgsqlDatabase")

    def connect(self):
        conn = psycopg2.connect(
            dbname=self.pg_database,
            user=self.pg_username,
            password=self.pg_password,
            host=self.pg_host,
            port=self.pg_port
        )
        conn.set_session(autocommit=True)  # Enable autocommit
        return conn




class RawDataTable:
    def __init__(self, db, schema, table_name, csv_file_path):
        self.db = db
        self.schema = schema
        self.table_name = table_name  
        self.csv_file_path = csv_file_path 

    def create_table(self):
        columns_definition = ', '.join(
            f'"{column_name}" {data_type}' for column_name, data_type in self.schema.items()
        )
        additional_columns = '"valid_from" TIMESTAMP, "valid_to" TIMESTAMP, "is_current" BOOLEAN, "row_hash" VARCHAR(64)'

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            {columns_definition},
            {additional_columns},
            PRIMARY KEY (property_id)
        );
        """
        try:
            with self.db.connect() as conn:
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {self.table_name};")  # Ensure the table is fresh each time
                cursor.execute(create_table_query)
            logging.info(f"Table {self.table_name} created successfully.")
        except psycopg2.Error as error:
            logging.error(f"An error occurred while creating the table: {error}")
            raise

    def load_data(self):
        # Open the CSV file for reading
        with open(self.csv_file_path, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            # Connect to the database
            with self.db.connect() as conn:
                cursor = conn.cursor()

                for row in reader:
                    # Construct the INSERT statement with ON CONFLICT clause
                    columns = ', '.join(row.keys())
                    values_placeholders = ', '.join(['%s'] * len(row))
                    insert_query = f"""
                    INSERT INTO {self.table_name} ({columns}) 
                    VALUES ({values_placeholders})
                    ON CONFLICT (property_id) DO NOTHING;
                    """

                    # Execute the INSERT statement
                    cursor.execute(insert_query, list(row.values()))
                
                # Commit the transaction to save all changes
                conn.commit()
                print(f"Data loaded into {self.table_name} successfully from {self.csv_file_path}.")






class StagingTable:
    def __init__(self, db, schema, table_name, csv_dir):
        self.db = db
        self.schema = schema
        self.table_name = table_name
        self.csv_dir = csv_dir

    @staticmethod
    def calculate_row_hash(row):
        row_string = '|'.join(str(row.get(column, '')) for column in sorted(row))
        return hashlib.sha256(row_string.encode()).hexdigest()

    @cached_property
    def drop_table_query(self): # SQL query to drop the table if it already exists
        return f"DROP TABLE IF EXISTS {self.table_name};"
    
    @cached_property
    def columns(self): # Extract and concatenate column names from the table schema
        return ', '.join(self.schema.keys())
    
    @cached_property
    def create_table_query(self): 
        # Define the columns that should only exist once
        exclusive_columns = {'valid_from', 'valid_to', 'is_current', 'row_hash'}

        # Ensure the exclusive columns are not in the initial schema definition
        columns_definition = ', '.join(
            f'"{column_name}" {data_type} NOT NULL'
            for column_name, data_type in self.schema.items()
            if column_name not in exclusive_columns
        )

        # Add additional columns only if they don't exist in the schema definition
        additional_columns = [
            '"valid_from" TIMESTAMP NOT NULL',
            '"valid_to" TIMESTAMP',
            '"is_current" BOOLEAN NOT NULL',
            '"row_hash" VARCHAR(64) NOT NULL'
        ]

        # Combine the existing columns and additional columns
        all_columns_definition = ', '.join([columns_definition] + additional_columns)

        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            {all_columns_definition},
            PRIMARY KEY (property_id) 
        );
        """

    def create_table(self):
        print(self.create_table_query)
        try:
            with self.db.connect() as conn:
                cursor = conn.cursor()
                # cursor.execute(self.drop_table_query)
                cursor.execute(self.create_table_query)

            logging.info(f"[[ {self.table_name.upper()} ]]")
            logging.info(f"Table {self.db.pg_database}.{self.table_name} created successfully.")
        except psycopg2.Error as error:
            logging.error(f"An error occurred while creating the table: {error}")
            raise

    def extract_from_csv(self):
        try:
            logging.info(f"Starting import of data from CSV files to {self.table_name}")
            with self.db.connect() as conn:
                total_rows_imported = self.extraction_process(conn)
            logging.info(f"All data imported successfully. {total_rows_imported} new rows added.")
            return total_rows_imported
        except psycopg2.Error as error:
            logging.error(f"An error occurred while importing data: {error}")
            raise

    def extraction_process(self, conn):
        self._truncate_table(conn)
        total_rows_imported, total_rows_updated = self._import_all_files(conn)  # Unpack the returned tuple
        if total_rows_imported == 0 and total_rows_updated == 0:
            raise Exception("No data was imported or updated. Exiting the program!")
        conn.commit()
        return total_rows_imported, total_rows_updated


    def _truncate_table(self, conn): # Remove all records from the table
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {self.table_name}")

    def _get_csv_files(self): # Get a list of all CSV filenames that match the table name from the specified directory
        return filter(lambda f: f.startswith("staging") and f.endswith(".csv"), os.listdir(self.csv_dir))

    def _get_full_csv_path(self, filename): 
        return os.path.join(self.csv_dir.replace('\\', '\\\\'), filename)

    def _import_all_files(self, conn):  # Import data from all CSV files that match the table name in the specified directory
        total_rows_imported = 0  # Counter for total number of rows imported
        total_rows_updated = 0  # Counter for total number of rows updated

        for filename in self._get_csv_files():
            csv_file_path = self._get_full_csv_path(filename)
            rows_imported, rows_updated = self._load_csv_to_db(conn, csv_file_path)  # Unpack the tuple
            total_rows_imported += rows_imported
            total_rows_updated += rows_updated

        return total_rows_imported, total_rows_updated  # Return counts of imported and updated rows

    def table_has_rows(self):
        with self.db.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(1) FROM {self.table_name}")
            return cursor.fetchone()[0] > 0

    def incremental_update(self):
        logging.info("Starting direct load/update...")
        try:
            with self.db.connect() as conn:
                if self.table_has_rows():
                    rows_imported, rows_updated = self.update_existing_data()
                else:
                    self.create_table()
                    rows_imported, rows_updated = self.extract_from_csv()

                logging.info(f"{rows_imported} new rows inserted.")
                logging.info(f"{rows_updated} rows updated.")
                return rows_imported, rows_updated

        except Exception as e:
            logging.error(f"An error occurred during the incremental update: {e}")
            raise

    def update_staging_table(self, raw_table_name):
        """
        Update the staging table with the latest data from the raw table.
        This method will insert new records and update existing ones as needed.
        """
        try:
            with self.db.connect() as conn:
                cursor = conn.cursor()

                # Insert or update records in the staging table based on the raw data
                cursor.execute(f"""
                INSERT INTO {self.table_name} (
                    property_id, page_link, source, agent_name, state, area, house_price,
                    price_square_feet, house_name, house_location, house_type, lot_type,
                    square_footage, house_furniture, posted_date, created_at, valid_from,
                    is_current, row_hash
                )
                SELECT
                    raw.property_id, raw.page_link, raw.source, raw.agent_name, raw.state, raw.area, raw.house_price,
                    raw.price_square_feet, raw.house_name, raw.house_location, raw.house_type, raw.lot_type,
                    raw.square_footage, raw.house_furniture, raw.posted_date, raw.created_at, CURRENT_TIMESTAMP,
                    TRUE, md5(random()::text)
                FROM {raw_table_name} raw
                ON CONFLICT (property_id) DO UPDATE SET
                    page_link = EXCLUDED.page_link,
                    source = EXCLUDED.source,
                    agent_name = EXCLUDED.agent_name,
                    state = EXCLUDED.state,
                    area = EXCLUDED.area,
                    house_price = EXCLUDED.house_price,
                    price_square_feet = EXCLUDED.price_square_feet,
                    house_name = EXCLUDED.house_name,
                    house_location = EXCLUDED.house_location,
                    house_type = EXCLUDED.house_type,
                    lot_type = EXCLUDED.lot_type,
                    square_footage = EXCLUDED.square_footage,
                    house_furniture = EXCLUDED.house_furniture,
                    posted_date = EXCLUDED.posted_date,
                    created_at = EXCLUDED.created_at,
                    valid_from = CURRENT_TIMESTAMP,
                    is_current = TRUE,
                    row_hash = md5(random()::text);
                """)

                conn.commit()
                logging.info("Staging table updated successfully.")
        except psycopg2.Error as error:
            logging.error(f"An error occurred while updating the staging table: {error}")
            raise



class MainExecutor:
    def __init__(self, config, db):
        self.config = config
        self.db = db
        # Load the entire schema data from the file
        self.schema_data = self.config.get_schema_data('pgsql_iproperty')
        self.logger = Logger(config.get_dataset_name(self.schema_data), config.log_dir).get_logger()

    def execute(self):
        self.logger.info("Script execution started.")

        # Debugging: Print the schema data to check its structure
        print("Loaded schema data:", self.schema_data)

        # Assuming this is where your CSV files are located
        csv_dir = self.config.out_dir  
        csv_file_name = 'staging_data.csv'  # Replace with your actual CSV file name
        csv_file_path = os.path.join(csv_dir, csv_file_name)

        # Set up raw and staging tables using the correct schema
        # In MainExecutor's execute method
        raw_data_table = RawDataTable(self.db, self.schema_data['raw_iproperty'], 'raw_iproperty', csv_file_path)
        staging_table = StagingTable(self.db, self.schema_data['staging_iproperty'], 'staging_iproperty', csv_dir)

        # Ensure the staging table is created first
        staging_table.create_table()

        # Load data into the raw table
        raw_data_table.create_table()
        raw_data_table.load_data()

        # Then update staging from raw
        staging_table.update_staging_table('raw_iproperty')

        self.logger.info("Execution completed.")

if __name__ == "__main__":
    load_dotenv('../../.env')  # Adjust the path as necessary
    config = Config()
    db = Database()
    executor = MainExecutor(config, db)
    executor.execute()

