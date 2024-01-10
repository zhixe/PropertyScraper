import schedule, time, os, logging, datetime, runpy
from dotenv import load_dotenv

# Load environment variables
dotenv_path = os.path.join(os.getcwd(), r'../.env')
load_dotenv(dotenv_path)

# Get paths from environment variables
main_dir = os.getenv("MAIN_DIR")
transform_dir = os.getenv("TRANSFORM")
script_file = os.path.join(transform_dir, 'transform.py')

# Ensure the script file path is correct
print(f"Script to run: {script_file}")

# Create a logs directory if it doesn't exist
log_dir = os.path.join(main_dir, 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Get the current date and time to include in the filename
current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Configure logging
# log_filename = f'{log_dir}/log_01_EXTRACT_{current_time}.log'
# logging.basicConfig(filename=log_filename,
#                     level=logging.INFO,
#                     format='%(asctime)s: %(levelname)s: %(message)s')

def job():
    logging.info("Starting the transformation task...")
    try:
        # Run the script using the full path to the Python executable
        # Modify the path to python.exe as per your environment
        os.chdir(transform_dir)
        runpy.run_path(script_file)
        logging.info("Transformation script has been initiated.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Scheduled task triggered.")

# Define schedules for the job
# schedule.every(1).minutes.do(job)  # Every 5 minutes
# schedule.every(90).minutes.do(job)  # Every 60 minutes
# schedule.every(510).minutes.do(job)  # Every 60 minutes
schedule.every().day.at("09:50").do(job)  # Every day at 9:50 AM

# Loop to keep the scheduler running
print("Scheduler is running...")
os.chdir(transform_dir)

while True:
    schedule.run_pending()
    time.sleep(1)
