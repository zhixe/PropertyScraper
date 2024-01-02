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
        return psycopg2.connect(
            dbname=self.pg_database,
            user=self.pg_username,
            password=self.pg_password,
            host=self.pg_host,
            port=self.pg_port
        )



# class TempStagingTable:
#     def __init__(self, engine, schema, table_name):
#         self.engine = engine
#         self.schema = schema
#         self.table_name = f"temp_{table_name}"

#     def create_temp_table(self):
#         # Define the columns that should only exist once
#         exclusive_columns = {'valid_from', 'valid_to', 'is_current', 'row_hash'}

#         # Ensure the exclusive columns are not in the initial schema definition
#         columns_definition = ', '.join(
#             f'"{column_name}" {data_type} NOT NULL'
#             for column_name, data_type in self.schema.items()
#             if column_name not in exclusive_columns
#         )

#         # Add additional columns only if they don't exist in the schema definition
#         additional_columns = []
#         if 'valid_from' not in self.schema:
#             additional_columns.append('"valid_from" TIMESTAMP')
#         if 'valid_to' not in self.schema:
#             additional_columns.append('"valid_to" TIMESTAMP')
#         if 'is_current' not in self.schema:
#             additional_columns.append('"is_current" BOOLEAN')
#         if 'row_hash' not in self.schema:
#             additional_columns.append('"row_hash" VARCHAR(64)')

#         # Combine the existing columns and additional columns
#         all_columns_definition = ', '.join([columns_definition] + additional_columns)

#         create_table_sql = f"""
#         DROP TABLE IF EXISTS {self.table_name};
#         CREATE TEMP TABLE {self.table_name} (
#             {all_columns_definition}
#         );
#         """
#         with self.engine.connect() as conn:
#             conn.execute(text(create_table_sql))
#         logging.info(f"Temporary table {self.table_name} created successfully.")


#     def drop_temp_table(self):
#         drop_table_sql = f"DROP TABLE IF EXISTS {self.table_name};"
#         with self.engine.connect() as conn:
#             conn.execute(text(drop_table_sql))



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


    @functools.lru_cache(maxsize=None)
    def load_data_query(self, csv_file_path):
        return f"""
            COPY {self.table_name}({self.columns})
            FROM '{csv_file_path}'
            DELIMITER ','
            CSV HEADER;
        """

    def create_table(self): # Create the table in the mssql database
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

    def _get_full_csv_path(self, filename): # Return the full path for a given CSV filename
        return os.path.join(self.csv_dir.replace('\\', '\\\\'), filename)

    def _load_csv_to_db(self, conn, csv_file_path):
        rows_imported = 0  # Initialize a counter for the rows imported
        rows_updated = 0  # Initialize a counter for the rows updated
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            for row in reader:
                cursor = conn.cursor()

                # Create a tuple of data for this row
                data_tuple = (
                    row['Property_ID'], row['Page_Link'], row['Source'], row['Agent_Name'],
                    row['State'], row['Area'], row['House_Price'], row['Price_Square_Feet'],
                    row['House_Name'], row['House_Location'], row['House_Type'], row['Lot_Type'],
                    row['Square_Footage'], row['House_Furniture'], row['Posted_Date'],
                    row['Created_At'], self.calculate_row_hash(row)
                )

                # INSERT or UPDATE statement here
                insert_update_query = sql.SQL("""
                INSERT INTO {table} (
                    property_id, page_link, source, agent_name, state, area, house_price,
                    price_square_feet, house_name, house_location, house_type, lot_type,
                    square_footage, house_furniture, posted_date, created_at, valid_from,
                    is_current, row_hash
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    CURRENT_TIMESTAMP, TRUE, %s
                )
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
                    row_hash = EXCLUDED.row_hash;
                """).format(table=sql.Identifier(self.table_name))

                # Logging the action for diagnostics
                logging.info(f"Processing row with property_id: {row['Property_ID']}")

                cursor.execute(insert_update_query, data_tuple)

                # Increment counters based on the operation performed
                if cursor.rowcount == 1:
                    rows_imported += 1
                    logging.info(f"Inserted row with property_id: {row['Property_ID']}")
                else:
                    rows_updated += cursor.rowcount - 1
                    logging.info(f"Updated row with property_id: {row['Property_ID']}")

                conn.commit()

        logging.info(f"Total rows imported: {rows_imported}, Total rows updated: {rows_updated}")
        return rows_imported, rows_updated  # Return counts of imported and updated rows


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
        if self.table_has_rows():
            rows_imported, rows_updated = self.update_existing_data()  # Unpack two values
        else:
            self.create_table()
            rows_imported, rows_updated = self.extract_from_csv()
        
        logging.info(f"{rows_imported} new rows inserted.")
        logging.info(f"{rows_updated} rows updated.")
        return rows_imported, rows_updated

    def update_existing_data(self):
        with self.db.connect() as conn:
            cursor = conn.cursor()

            # Define the SQL for inserting and on conflict updating
            # Adjust this SQL to match your PostgreSQL version and requirements
            sql = f"""
            INSERT INTO {self.table_name} (
                property_id, page_link, source, agent_name, state, area, house_price,
                price_square_feet, house_name, house_location, house_type, lot_type,
                square_footage, house_furniture, posted_date, created_at, valid_from,
                is_current, row_hash
            )
            SELECT
                property_id, page_link, source, agent_name, state, area, house_price,
                price_square_feet, house_name, house_location, house_type, lot_type,
                square_footage, house_furniture, posted_date, created_at, CURRENT_TIMESTAMP,
                TRUE, row_hash
            FROM {self.table_name}
            ON CONFLICT (property_id) DO UPDATE SET
                page_link = EXCLUDED.page_link,
                source = EXCLUDED.source,
                agent_name = EXCLUDED.agent_name,
                state = EXCLUDED.state,
                area = EXCLUDED.area,
                house_price = CASE WHEN {self.table_name}.house_price <> EXCLUDED.house_price AND {self.table_name}.posted_date < EXCLUDED.posted_date THEN EXCLUDED.house_price ELSE {self.table_name}.house_price END,
                price_square_feet = EXCLUDED.price_square_feet,
                house_name = EXCLUDED.house_name,
                house_location = EXCLUDED.house_location,
                house_type = EXCLUDED.house_type,
                lot_type = EXCLUDED.lot_type,
                square_footage = EXCLUDED.square_footage,
                house_furniture = EXCLUDED.house_furniture,
                posted_date = CASE WHEN {self.table_name}.posted_date < EXCLUDED.posted_date THEN EXCLUDED.posted_date ELSE {self.table_name}.posted_date END,
                created_at = EXCLUDED.created_at,
                valid_from = CURRENT_TIMESTAMP,
                is_current = TRUE,
                row_hash = EXCLUDED.row_hash;
            """

            # Execute the UPSERT command
            cursor.execute(sql)
            affected_rows = cursor.rowcount

            conn.commit()

            # Assuming all affected rows are updates as it's an existing table
            logging.info(f"Rows updated: {affected_rows}")
            return 0, affected_rows  # Assuming no new rows inserted, all are updates



class MainExecutor:
    def __init__(self): # Initialize the configuration and set up the required environment for the execution
        self.config = Config()
        self._setup_environment()

    @functools.cached_property
    def schema_data(self): # Return the schema data associated with the current script's filename
        return self.config.get_schema_data(self.script_basename)

    @functools.cached_property
    def script_basename(self): # Get the base name (filename without the extension) of the current script
        # return os.path.splitext(os.path.basename(__file__))[0]
        # Split the script name on the underscore and remove the file extension
        parts = os.path.basename(__file__).split('_')
        return os.path.splitext('_'.join(parts[1:]))[0]

    def _setup_database(self): # Initialize and set up the database connection
        self.db = Database()

    def _setup_logger(self): # Set up the logging mechanism using the dataset name derived from the schema and the log directory from the configuration
        self.dataset_name = self.config.get_dataset_name(self.schema_data)
        self.logger = Logger(self.dataset_name, self.config.log_dir)

    def _setup_csv_to_mssql(self): # Set up the CSV to mssql transfer mechanism by initializing the relevant class with necessary parameters
        csv_dir = os.path.join(os.getenv("MAIN_DIR"), os.getenv("STAGING_DIR"), '')
        self.csv_to_mssql = StagingTable(csv_dir, self.schema_data, self.db, self.dataset_name)
        schema_data = self.config.get_schema_data(self.script_basename)
        key_name = self.config.key_name  # Use the property from config

        if key_name in schema_data:  # Check if 'key_name' key exists
            self.csv_to_mssql = StagingTable(csv_dir, schema_data[key_name], self.db, self.dataset_name)
        else:
            raise KeyError(fr"{key_name} not found in schema data!")

    def _setup_environment(self):
        self._setup_database()
        self._setup_logger()
        self._setup_staging_tables()

    def _setup_staging_tables(self):
        csv_dir = os.path.join(os.getenv("MAIN_DIR"), os.getenv("STAGING_DIR"), '')
        schema_data = self.config.get_schema_data(self.script_basename)
        key_name = self.config.key_name

        if key_name in schema_data:
            engine = create_engine(f'postgresql+psycopg2://{self.db.pg_username}:{self.db.pg_password}@{self.db.pg_host}/{self.db.pg_database}')
            # self.temp_staging_table = TempStagingTable(engine, schema_data[key_name], self.dataset_name)
            self.staging_table = StagingTable(self.db, schema_data[key_name], self.dataset_name, csv_dir)
        else:
            raise KeyError(f"{key_name} not found in schema data!")

    def execute(self):
        logger = self.logger.get_logger()
        logging.info("Script execution started.")
        self.staging_table.create_table()
        inserted, updated = self.staging_table.incremental_update()  # Adjusted call here
        logging.info(f"Execution completed: {inserted} rows inserted, {updated} rows updated.")
        os.environ.clear()

if __name__ == "__main__":
    main_executor = MainExecutor()
    main_executor.execute()