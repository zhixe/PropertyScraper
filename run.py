import runpy, os, threading
from dotenv import load_dotenv

# Load environment variables
dotenv_path = os.path.join(os.getcwd(), r'.env')
load_dotenv(dotenv_path)

# Get paths from environment variables
main_dir = os.getenv("MAIN_DIR")
auto_dir = os.getenv("AUTO_DIR")
auto_file_extract = os.path.join(auto_dir, '01_extract_auto.py')
auto_file_transform = os.path.join(auto_dir, '02_transform_auto.py')
auto_file_load = os.path.join(auto_dir, '03_load_auto.py')
auto_file_olap = os.path.join(auto_dir, '04_olap_auto.py')


def run_script(path):
    runpy.run_path(path)

# List of file paths
file_paths = [auto_file_extract, auto_file_transform, auto_file_load, runpy.run_path(auto_file_olap)
]

# Create and start a new thread for each file
for path in file_paths:
    thread = threading.Thread(target=run_script, args=(path,))
    thread.start()


