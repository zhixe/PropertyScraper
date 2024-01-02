import os, logging, functools, psycopg2
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

class Config:
    def __init__(self):
        self.load_environment_variables()

    def load_environment_variables(self):
        dotenv_path = os.path.join(os.getcwd(), '../../.env')
        load_dotenv(dotenv_path)

    @property
    def log_dir(self):
        return os.path.join(os.getenv("MAIN_DIR"), os.getenv("LOG_DIR"))


class Logger:
    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.setup_logging()

    def get_log_filename(self):
        return f"{self.log_dir}/log_OLAP_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    def setup_logging(self):
        log_filename = self.get_log_filename()
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename=log_filename)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(console_formatter)
        logging.getLogger().addHandler(console_handler)


class Database:
    def __init__(self):
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


class OLAPProcessor:
    def __init__(self, db):
        self.db = db

    def create_fnd_tables(self):
        fnd_tables_sql = [
            """
                -- Dimension Table for States
                DROP TABLE IF EXISTS dim_state;
                CREATE TABLE IF NOT EXISTS dim_state (
                    state_id SERIAL PRIMARY KEY,
                    state_name VARCHAR(255) UNIQUE NOT NULL
                );

                -- Dimension Table for Areas
                DROP TABLE IF EXISTS dim_area;
                CREATE TABLE IF NOT EXISTS dim_area (
                    area_id SERIAL PRIMARY KEY,
                    area_name VARCHAR(255) UNIQUE NOT NULL,
                    state_id INT REFERENCES dim_state(state_id)
                );

                -- Dimension Table for Property Types
                DROP TABLE IF EXISTS dim_property_type;
                CREATE TABLE IF NOT EXISTS dim_property_type (
                    property_type_id SERIAL PRIMARY KEY,
                    property_type_name VARCHAR(255) UNIQUE NOT NULL
                );

                -- Dimension Table for Agents
                DROP TABLE IF EXISTS dim_agent;
                CREATE TABLE IF NOT EXISTS dim_agent (
                    agent_id SERIAL PRIMARY KEY,
                    agent_name VARCHAR(255) UNIQUE NOT NULL
                );

                -- Dimension Table for Dates
                DROP TABLE IF EXISTS dim_date;
                CREATE TABLE IF NOT EXISTS dim_date (
                    date_id SERIAL PRIMARY KEY,
                    date DATE UNIQUE NOT NULL,
                    day INT,
                    month INT,
                    year INT,
                    quarter INT,
                    day_of_week INT,
                    week_of_year INT
                );

                -- Dimension Table for Price Range
                DROP TABLE IF EXISTS dim_price_range;
                CREATE TABLE IF NOT EXISTS dim_price_range (
                    price_range_id SERIAL PRIMARY KEY,
                    range_label VARCHAR(255) UNIQUE NOT NULL
                );

                -- Dimension Table for Lot Types
                DROP TABLE IF EXISTS dim_lot_type;
                CREATE TABLE IF NOT EXISTS dim_lot_type (
                    lot_type_id SERIAL PRIMARY KEY,
                    lot_type_name VARCHAR(255) UNIQUE NOT NULL
                );

                -- Fact Table for Property Sales
                CREATE TABLE IF NOT EXISTS fact_property_sales (
                    sale_id SERIAL PRIMARY KEY,
                    property_id VARCHAR(255),  -- Ensure this matches the data type in the staging table
                    state_id INT REFERENCES dim_state(state_id),
                    area_id INT REFERENCES dim_area(area_id),
                    property_type_id INT REFERENCES dim_property_type(property_type_id),
                    agent_id INT REFERENCES dim_agent(agent_id),
                    sale_date DATE,
                    sale_price NUMERIC
                );
            """,
        ]
        conn = self.db.connect()
        cursor = conn.cursor()
        try:
            for sql in fnd_tables_sql:
                cursor.execute(sql)
                conn.commit()  # Commit after each command
                logging.info(f"Executed SQL: {sql}")
        except Exception as e:
            logging.error(f"Error executing SQL: {sql}, Error: {e}")
            conn.rollback()  # Rollback if any error occurs
        finally:
            cursor.close()
            conn.close()

    def populate_fnd_tables(self):
        # 1. State Dimension Table
        populate_dim_state = """
            -- Populating the State Dimension Table
            INSERT INTO dim_state (state_name)
            SELECT DISTINCT state
            FROM property.public.staging_iproperty
            WHERE state IS NOT NULL
            ON CONFLICT (state_name) DO NOTHING;
        """

        # 2. Area Dimension Table
        populate_dim_area = """
            -- Populating the Area Dimension Table
            INSERT INTO dim_area (area_name, state_id)
            SELECT DISTINCT area, ds.state_id
            FROM property.public.staging_iproperty sp
            INNER JOIN dim_state ds ON sp.state = ds.state_name
            WHERE sp.area IS NOT NULL
            ON CONFLICT (area_name) DO NOTHING;
        """

        # 3. Property Type Dimension Table
        populate_dim_property_type = """
            -- Populating the Property Type Dimension Table
            INSERT INTO dim_property_type (property_type_name)
            SELECT DISTINCT house_type
            FROM property.public.staging_iproperty
            WHERE house_type IS NOT NULL
            ON CONFLICT (property_type_name) DO NOTHING;
        """

        # 4. Agent Dimension Table
        populate_dim_agent = """
            -- Populating the Agent Dimension Table
            INSERT INTO dim_agent (agent_name)
            SELECT DISTINCT agent_name
            FROM property.public.staging_iproperty
            WHERE agent_name IS NOT NULL
            ON CONFLICT (agent_name) DO NOTHING;
        """

        # 5. Date Dimension Table
        populate_dim_date = """
            -- Populating Date Dimension Table from staging table
            INSERT INTO dim_date (date, day, month, year, quarter, day_of_week, week_of_year)
            SELECT DISTINCT
                posted_date::date AS date,
                EXTRACT(DAY FROM posted_date) AS day,
                EXTRACT(MONTH FROM posted_date) AS month,
                EXTRACT(YEAR FROM posted_date) AS year,
                EXTRACT(QUARTER FROM posted_date) AS quarter,
                EXTRACT(DOW FROM posted_date) AS day_of_week,
                EXTRACT(WEEK FROM posted_date) AS week_of_year
            FROM property.public.staging_iproperty
            WHERE posted_date IS NOT NULL
            ON CONFLICT (date) DO NOTHING;
        """

        # 6. Price Range Dimension Table
        populate_dim_price_range = """
            -- Populating Price Range Dimension Table
            INSERT INTO dim_price_range (range_label) VALUES ('0-500k'), ('500k-1M'), ('1M-1.5M'), ('1.5M+');
        """

        # 7. Lot Type Dimension Table
        populate_dim_lot_type = """
            -- Populating Lot Type Dimension Table
            INSERT INTO dim_lot_type (lot_type_name)
            SELECT DISTINCT lot_type
            FROM property.public.staging_iproperty
            WHERE lot_type IS NOT NULL
            ON CONFLICT (lot_type_name) DO NOTHING;
        """

        # 8. Property Sales Fact Table
        populate_fact_property_sales = """
            -- Populating the Agent Dimension Table
            INSERT INTO fact_property_sales (property_id, state_id, area_id, property_type_id, agent_id, sale_date, sale_price)
            SELECT 
                sp.property_id,
                ds.state_id, 
                da.area_id, 
                dpt.property_type_id, 
                dag.agent_id, 
                sp.posted_date::date,
                sp.house_price
            FROM property.public.staging_iproperty sp
            INNER JOIN dim_state ds ON sp.state = ds.state_name
            INNER JOIN dim_area da ON sp.area = da.area_name
            INNER JOIN dim_property_type dpt ON sp.house_type = dpt.property_type_name
            INNER JOIN dim_agent dag ON sp.agent_name = dag.agent_name;
                    """

        # Combine all SQL commands into a list
        all_populate_dnf_sqls = [
            populate_dim_state,
            populate_dim_area,
            populate_dim_property_type,
            populate_dim_agent,
            populate_dim_date,
            populate_dim_price_range,
            populate_dim_lot_type,
            populate_fact_property_sales
        ]

        # Execute each SQL command
        conn = self.db.connect()  # Get the psycopg2 connection
        cursor = conn.cursor()
        try:
            for sql in all_populate_dnf_sqls:
                cursor.execute(sql)
                conn.commit()  # Commit after each command
                logging.info(f"Populated Dimension and Fact tables with SQL: {sql}")
        except Exception as e:
            logging.error(f"Error executing SQL: {e}")
            conn.rollback()  # Rollback if any error occurs
        finally:
            cursor.close()
            conn.close()

    def create_olap_tables(self):
        olap_tables_sql = [
            """
            DROP TABLE IF EXISTS property.olap.olap_time_series_prices;
            CREATE TABLE IF NOT EXISTS property.olap.olap_time_series_prices (
                period TIMESTAMP,
                average_house_price DECIMAL,
                count INT
            );
            """,
            """
            DROP TABLE IF EXISTS property.olap.olap_furniture_status;
            CREATE TABLE IF NOT EXISTS property.olap.olap_furniture_status (
                furniture_status VARCHAR(100),
                count INT
            );
            """,
            """
            DROP TABLE IF EXISTS property.olap.olap_avg_price_by_state;
            CREATE TABLE IF NOT EXISTS property.olap.olap_avg_price_by_state (
                state VARCHAR(100),
                average_price DECIMAL,
                count INT
            );
            """,
            """
            DROP TABLE IF EXISTS property.olap.olap_price_trends_by_type;
            CREATE TABLE IF NOT EXISTS property.olap.olap_price_trends_by_type (
                property_type VARCHAR(100),
                average_price DECIMAL,
                count INT
            );
            """,
            """
            DROP TABLE IF EXISTS property.olap.olap_property_type_distribution;
            CREATE TABLE IF NOT EXISTS property.olap.olap_property_type_distribution (
                property_type VARCHAR(100),
                count INT
            );
            """,
            """
            DROP TABLE IF EXISTS property.olap.olap_posting_dates;
            CREATE TABLE IF NOT EXISTS property.olap.olap_posting_dates (
                posting_date DATE,
                count INT
            );
            """,
            """
            DROP TABLE IF EXISTS property.olap.olap_property_for_sales_summary;
            CREATE TABLE IF NOT EXISTS property.olap.olap_detailed_sales_summary (
                posted_date DATE,
                property_type VARCHAR(255),
                state VARCHAR(255),
                agent_name VARCHAR(255),
                area VARCHAR(255),
                average_price DECIMAL,
                total_sales INT
            );
            """,
            """
            DROP TABLE IF EXISTS property.olap.olap_agent_summary;
            CREATE TABLE IF NOT EXISTS property.olap.olap_agent_summary (
                agent_id INT REFERENCES dim_agent(agent_id),
                agent_name VARCHAR(255),
                total_sales NUMERIC,
                average_sale_price DECIMAL,
                properties_listed INT
            );
            """
        ]
        conn = self.db.connect()  # Get the psycopg2 connection
        cursor = conn.cursor()
        try:
            for sql in olap_tables_sql:
                cursor.execute(sql)
                conn.commit()  # Commit after each command
                logging.info(f"Executed SQL: {sql}")
        except Exception as e:
            logging.error(f"Error executing SQL: {sql}, Error: {e}")
            conn.rollback()  # Rollback if any error occurs
        finally:
            cursor.close()
            conn.close()

    def populate_olap_tables(self):
        # 1. Average House Prices by Date and Count of Properties
        populate_time_series_prices_sql = """
        CREATE TEMP TABLE df_average AS
        SELECT 
            DATE_TRUNC('month', posted_date) AS posted_date_month,
            AVG(house_price) AS house_price_avg
        FROM property.public.staging_iproperty
        GROUP BY posted_date_month;

        CREATE TEMP TABLE df_count AS
        SELECT
            DATE_TRUNC('month', posted_date) AS posted_date_month,
            COUNT(property_id) AS property_id_count
        FROM property.public.staging_iproperty
        GROUP BY posted_date_month;

        INSERT INTO property.olap.olap_time_series_prices (period, average_house_price, count)
        SELECT 
            df_average.posted_date_month,
            df_average.house_price_avg,
            df_count.property_id_count
        FROM df_average
        FULL OUTER JOIN 
            df_count ON df_average.posted_date_month = df_count.posted_date_month;

        DROP TABLE IF EXISTS df_average;
        DROP TABLE IF EXISTS df_count;
        """

        # 2. Average House Prices by State and Count of Properties
        populate_avg_price_by_state_sql = """
        CREATE TEMP TABLE df_average AS
        SELECT 
            state,
            AVG(house_price) AS house_price_avg
        FROM property.public.staging_iproperty
        GROUP BY state;

        CREATE TEMP TABLE df_count AS
        SELECT 
            state,
            COUNT(property_id) AS property_id_count
        FROM property.public.staging_iproperty
        GROUP BY state;

        INSERT INTO property.olap.olap_avg_price_by_state (state, average_price, count)
        SELECT 
            df_average.state,
            df_average.house_price_avg,
            df_count.property_id_count
        FROM df_average
        FULL OUTER JOIN 
            df_count ON df_average.state = df_count.state;

        DROP TABLE IF EXISTS df_average;
        DROP TABLE IF EXISTS df_count;
        """

        # 3. Average House Prices by House Type and Count of Properties
        populate_price_trends_by_type_sql = """
        CREATE TEMP TABLE df_average2 AS
        SELECT 
            house_type,
            AVG(house_price) AS house_price_avg
        FROM property.public.staging_iproperty
        GROUP BY house_type;

        CREATE TEMP TABLE df_count2 AS
        SELECT 
            house_type,
            COUNT(property_id) AS property_id_count
        FROM property.public.staging_iproperty
        GROUP BY house_type;

        INSERT INTO property.olap.olap_price_trends_by_type (property_type, average_price, count)
        SELECT 
            df_average2.house_type,
            df_average2.house_price_avg,
            df_count2.property_id_count
        FROM df_average2
        FULL OUTER JOIN 
            df_count2 ON df_average2.house_type = df_count2.house_type;
        """

        # 4. Distribution of Property Type
        populate_property_type_distribution_sql = """
        INSERT INTO property.olap.olap_property_type_distribution (property_type, count)
        SELECT
            house_type,
            COUNT(*) AS count
        FROM property.public.staging_iproperty
        GROUP BY house_type;
        """

        # 5. Distribution of Furniture Availability
        populate_furniture_status_sql = """
        INSERT INTO property.olap.olap_furniture_status (furniture_status, count)
        SELECT
            house_furniture,
            COUNT(*) AS count
        FROM property.public.staging_iproperty
        GROUP BY house_furniture;
        """

        # 6. Distribution of Posting Dates
        populate_posting_dates_sql = """
        INSERT INTO property.olap.olap_posting_dates (posting_date, count)
        SELECT
            DATE(posted_date) AS posting_date,
            COUNT(*) AS count
        FROM property.public.staging_iproperty
        GROUP BY DATE(posted_date);
        """

        # 7. Distribution of Posting Dates
        # populate_olap_property_sales_summary_sql  = """
        # INSERT INTO property.olap.olap_detailed_sales_summary (posted_date, property_type, state, agent_name, area, average_price, total_sales)
        # SELECT 
        #     sp.posted_date,
        #     dpt.property_type_name,
        #     ds.state_name,
        #     da.agent_name,
        #     dar.area_name,
        #     AVG(sp.house_price) AS average_price,
        #     COUNT(sp.property_id) AS total_sales
        # FROM property.public.staging_iproperty sp
        # INNER JOIN dim_state ds ON sp.state = ds.state_name
        # INNER JOIN dim_area dar ON sp.area = dar.area_name
        # INNER JOIN dim_property_type dpt ON sp.house_type = dpt.property_type_name
        # INNER JOIN dim_agent da ON sp.agent_name = da.agent_name
        # GROUP BY sp.posted_date, dpt.property_type_name, ds.state_name, da.agent_name, dar.area_name;
        # """

        populate_olap_agent_summary_sql = """
        INSERT INTO property.olap.olap_agent_summary (agent_id, agent_name, total_sales, average_sale_price, properties_listed)
        SELECT 
            da.agent_id,
            da.agent_name,
            SUM(fps.sale_price) AS total_sales,
            AVG(fps.sale_price) AS average_sale_price,
            COUNT(fps.property_id) AS properties_listed
        FROM fact_property_sales fps
        INNER JOIN dim_agent da ON fps.agent_id = da.agent_id
        GROUP BY da.agent_id, da.agent_name;
        """

        # Combine all SQL commands into a list
        all_populate_olap_sqls = [
            populate_time_series_prices_sql,
            populate_avg_price_by_state_sql,
            populate_price_trends_by_type_sql,
            populate_property_type_distribution_sql,
            populate_furniture_status_sql,
            populate_posting_dates_sql,
            # populate_olap_property_sales_summary_sql,
            populate_olap_agent_summary_sql
        ]

        # Execute each SQL command
        conn = self.db.connect()  # Get the psycopg2 connection
        cursor = conn.cursor()
        try:
            for sql in all_populate_olap_sqls:
                cursor.execute(sql)
                conn.commit()  # Commit after each command
                logging.info(f"Populated OLAP tables with SQL: {sql}")
        except Exception as e:
            logging.error(f"Error executing SQL: {e}")
            conn.rollback()  # Rollback if any error occurs
        finally:
            cursor.close()
            conn.close()

    def create_indexes(self):
        index_queries = [
            # Indexes for Dimension Tables
            "CREATE INDEX IF NOT EXISTS idx_state_name ON dim_state(state_name);",
            "CREATE INDEX IF NOT EXISTS idx_area_name_state_id ON dim_area(area_name, state_id);",
            "CREATE INDEX IF NOT EXISTS idx_property_type_name ON dim_property_type(property_type_name);",
            "CREATE INDEX IF NOT EXISTS idx_agent_name ON dim_agent(agent_name);",
            "CREATE INDEX IF NOT EXISTS idx_date ON dim_date(date);",
            "CREATE INDEX IF NOT EXISTS idx_price_range_label ON dim_price_range(range_label);",
            "CREATE INDEX IF NOT EXISTS idx_lot_type_name ON dim_lot_type(lot_type_name);",
            
            # Indexes for Fact Table
            "CREATE INDEX IF NOT EXISTS idx_property_id ON fact_property_sales(property_id);",
            "CREATE INDEX IF NOT EXISTS idx_sale_date ON fact_property_sales(sale_date);",
            
            # Indexes for OLAP Table
            "CREATE INDEX IF NOT EXISTS idx_olap_furniture_status_status ON property.olap.olap_furniture_status(furniture_status);",
            "CREATE INDEX IF NOT EXISTS idx_olap_avg_price_by_state_state ON property.olap.olap_avg_price_by_state(state);",
            "CREATE INDEX IF NOT EXISTS idx_olap_price_trends_by_type_type ON property.olap.olap_price_trends_by_type(property_type);",
            "CREATE INDEX IF NOT EXISTS idx_olap_property_type_distribution_type ON property.olap.olap_property_type_distribution(property_type);",
            "CREATE INDEX IF NOT EXISTS idx_olap_posting_dates_date ON property.olap.olap_posting_dates(posting_date);",
            "CREATE INDEX IF NOT EXISTS idx_olap_detailed_sales_summary_date ON property.olap.olap_detailed_sales_summary(posted_date);",
            "CREATE INDEX IF NOT EXISTS idx_olap_agent_summary_agent_id ON property.olap.olap_agent_summary(agent_id);",
        ]

        conn = self.db.connect()
        cursor = conn.cursor()
        try:
            for query in index_queries:
                cursor.execute(query)
                conn.commit()
                logging.info(f"Executed Indexing: {query}")
        except Exception as e:
            logging.error(f"Error executing Indexing: {e}")
            conn.rollback()  # Rollback if any error occurs
        finally:
            cursor.close()
            conn.close()

    def create_materialized_views(self):
        mv_queries = [
            # # Materialized View for OLAP Detailed Sales Summary
            # """
            # CREATE MATERIALIZED VIEW IF NOT EXISTS property.olap.mv_olap_detailed_sales_summary AS
            # SELECT 
            #     sp.posted_date,
            #     dpt.property_type_name AS property_type,
            #     ds.state_name AS state,
            #     da.agent_name,
            #     dar.area_name AS area,
            #     AVG(sp.house_price) AS average_price,
            #     COUNT(sp.property_id) AS total_sales
            # FROM property.public.staging_iproperty sp
            # INNER JOIN dim_state ds ON sp.state = ds.state_name
            # INNER JOIN dim_area dar ON sp.area = dar.area_name
            # INNER JOIN dim_property_type dpt ON sp.house_type = dpt.property_type_name
            # INNER JOIN dim_agent da ON sp.agent_name = da.agent_name
            # GROUP BY sp.posted_date, dpt.property_type_name, ds.state_name, da.agent_name, dar.area_name;
            # """,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS property.olap.mv_olap_time_series_prices AS
            SELECT * FROM property.olap.olap_time_series_prices;
            """,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS property.olap.mv_olap_furniture_status AS
            SELECT * FROM property.olap.olap_furniture_status;
            """,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS property.olap.mv_olap_avg_price_by_state AS
            SELECT * FROM property.olap.olap_avg_price_by_state;
            """,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS property.olap.mv_olap_price_trends_by_type AS
            SELECT * FROM property.olap.olap_price_trends_by_type;
            """,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS property.olap.mv_olap_property_type_distribution AS
            SELECT * FROM property.olap.olap_property_type_distribution;
            """,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS property.olap.mv_olap_posting_dates AS
            SELECT * FROM property.olap.olap_posting_dates;
            """,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS property.olap.mv_olap_detailed_sales_summary AS
            SELECT * FROM property.olap.olap_detailed_sales_summary;
            """,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS property.olap.mv_olap_agent_summary AS
            SELECT * FROM property.olap.olap_agent_summary;
            """,
        ]

        conn = self.db.connect()
        cursor = conn.cursor()
        try:
            for query in mv_queries:
                cursor.execute(query)
                conn.commit()
                logging.info("Created Materialized View: {0}".format(query))
        except Exception as e:
            logging.error("Error creating Materialized View: {0}, Error: {1}".format(query, e))
            conn.rollback()  # Rollback if any error occurs
        finally:
            cursor.close()
            conn.close()

    # def refresh_materialized_views(self):
    #     refresh_queries = [
    #         "REFRESH MATERIALIZED VIEW property.olap.mv_olap_detailed_sales_summary;",
    #     ]

    #     conn = self.db.connect()
    #     cursor = conn.cursor()
    #     try:
    #         for query in refresh_queries:
    #             cursor.execute(query)
    #             conn.commit()
    #             logging.info("Refreshed Materialized View: {0}".format(query))
    #     except Exception as e:
    #         logging.error("Error refreshing Materialized View: {0}, Error: {1}".format(query, e))
    #         conn.rollback()
    #     finally:
    #         cursor.close()
    #         conn.close()


class MainExecutor:
    def __init__(self):
        self.config = Config()
        self.logger = Logger(self.config.log_dir)
        self.db = Database()
        self.olap_processor = OLAPProcessor(self.db)

    def refresh_views(self):
        self.olap_processor.refresh_materialized_views()
        logging.info("Materialized Views refreshed.")
        # USE CRONTAB IN LINUX TO REFRESH VIEW
        # 0 3 * * * /usr/bin/python /path/to/script.py refresh_views

    def execute(self):
        self.olap_processor.create_fnd_tables()
        self.olap_processor.populate_fnd_tables()
        self.olap_processor.create_olap_tables()
        self.olap_processor.populate_olap_tables()
        self.olap_processor.create_indexes()  
        self.olap_processor.create_materialized_views()
        logging.info("Materialized Views creation complete.")
        logging.info("OLAP processing complete.")


if __name__ == "__main__":
    main_executor = MainExecutor()
    main_executor.execute()