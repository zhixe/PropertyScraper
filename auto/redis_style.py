from celery import Celery
import runpy
import os
import logging
import datetime
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

# Get the current date and time to include in the filename
current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Configure logging
log_filename = f'{log_dir}/log_01_EXTRACT_{current_time}.log'
logging.basicConfig(filename=log_filename,
                    level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')

app = Celery('tasks', broker='redis://localhost:6379/0')  # Configure Celery with Redis as the broker

@app.task
def job():
    logging.info("Starting the web scraping task...")
    try:
        # Run the script using runpy
        runpy.run_path(script_file)
        logging.info("Web scraping script has been initiated.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Scheduled task triggered.")

# Schedule tasks (These are examples, you'd schedule as needed)
job.apply_async(countdown=5*60)  # Schedule to run in 5 minutes from now
job.apply_async(
    eta=datetime.datetime.now(datetime.timezone.utc)
    + datetime.timedelta(hours=12)
)
