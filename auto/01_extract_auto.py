import schedule, time, os, logging, datetime, runpy, glob
from dotenv import load_dotenv

# Load environment variables
dotenv_path = os.path.join(os.getcwd(), r'../.env')
load_dotenv(dotenv_path)

# Get paths from environment variables
main_dir = os.getenv("MAIN_DIR")
extract_dir = os.getenv("EXTRACT")
script_file = os.path.join(extract_dir, 'extract.py')

# Ensure the script file path is correct
print(f"Script to run: {script_file}")

# Create a logs directory if it doesn't exist
log_dir = os.path.join(main_dir, 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Get the list of CSV files
data_dir = os.path.join(main_dir, 'data/raw')
csv_files = glob.glob(os.path.join(data_dir, '*.csv'))

# Get the current date and time to include in the filename
current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Configure logging
# log_filename = f'{log_dir}/log_01_EXTRACT_{current_time}.log'
# logging.basicConfig(filename=log_filename,
#                     level=logging.INFO,
#                     format='%(asctime)s: %(levelname)s: %(message)s')

def job():
    logging.info("Starting the web scraping task...")
    try:
        # Run the script using the full path to the Python executable
        # Remove all previous CSV files in the data directory
        for csv_file in csv_files:
            os.remove(csv_file)
        os.chdir(extract_dir)
        runpy.run_path(script_file)  # Run the script
        logging.info("Web scraping script has been initiated.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Scheduled task triggered.")

# Define schedules for the job
# schedule.every(5).minutes.do(job)  # Every 5 minutes
# schedule.every(60).minutes.do(job)  # Every 60 minutes
# schedule.every(720).minutes.do(job)  # Every 12 hours
# schedule.every(8).hours.do(job)  # Every 60 minutes
# schedule.every(8).hours.at("09:00").do(job)  # Every 12 hours starting at 5 AM
schedule.every().day.at("09:00").do(job)  # Every day at 9 AM

# Loop to keep the scheduler running
print("Scheduler is running...")
os.chdir(extract_dir)

logging.info("Web scraping script has been initiated.")
runpy.run_path(script_file)  # Run the script
while True:
    schedule.run_pending()
    time.sleep(1)
