import json, subprocess, os, re, time, sys
from dotenv import load_dotenv

# Load environment variables from .env file
dotenv_path = os.path.join(os.getcwd(), r'../../.env')
load_dotenv(dotenv_path)
mainDir = os.getenv("MAIN_DIR")
schemaDir = os.path.join(mainDir, os.getenv("SCHEMA_DIR"))
schemaFile = os.path.join(schemaDir, 'script.json')
venvDir = os.path.join(mainDir, os.getenv("PY_ENV"))
dataDir = os.path.join(mainDir, 'data')

# Remove data directory
# if os.path.exists(dataDir):
#     shutil.rmtree(dataDir)

# Read the JSON file
with open(schemaFile, 'r') as json_file:
    data = json.load(json_file)

# Divide the scripts into batches of 4
script_list = list(data["Script"].items())
batch_size = 2 # Max 16, Ideal 4, Best 8

# Initialize a variable to store the total execution time
total_execution_time = 0

for batch_number, i in enumerate(range(0, len(script_list), batch_size), start=1):
    batch = script_list[i:i + batch_size]
    subprocesses = []  # Store the Popen objects

    for script_number, script_name in batch:
        print(f"Running script {script_number}: {script_name}")
        script_start_time = time.time()

        # Set the environment variable for the batch number
        os.environ['BATCH_NUMBER'] = str(batch_number)

        region_trim = re.search(r'_(.+?)/', script_name).group(1)
        number_decimal_trim = re.search(r'(\d+)_', script_name).group(1)
        number_trim = int(re.search(r'(\d+)_', script_name).group(1))
        os.environ['MY_REGION'] = str(region_trim)
        os.environ['SCRIPT_NUMBER_DECIMAL'] = number_decimal_trim
        os.environ['SCRIPT_NUMBER'] = str(number_trim)
        os.environ['SCRIPT_NAME'] = str(script_name)

        # Start the script as a subprocess
        subprocesses.append(subprocess.Popen(['python3', script_name]))

        print(f"Script {script_number} executed!")

    # Wait for all subprocesses in the batch to complete
    for process in subprocesses:
        process.wait()

    script_end_time = time.time()
    script_execution_time = script_end_time - script_start_time
    print(f"Script {script_number} executed in {script_execution_time} seconds.")
total_execution_time += script_execution_time
print(f"All scripts executed in {total_execution_time} seconds.")
sys.exit(0)
