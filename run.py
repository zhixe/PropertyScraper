import runpy, os
from dotenv import load_dotenv

# Load environment variables
dotenv_path = os.path.join(os.getcwd(), r'../.env')
load_dotenv(dotenv_path)

# Get paths from environment variables
main_dir = os.getenv("MAIN_DIR")
extract_dir = ""os.getenv("AUTO_DIR")
auto_file_extract = os.path.join(extract_dir, '01_extract_auto.py')
auto_file_transform = os.path.join(extract_dir, '02_transform_auto.py')
auto_file_load = os.path.join(extract_dir, '03_load_auto.py')
auto_file_olap = os.path.join(extract_dir, '04_olap_auto.py')


runpy.run_path(auto_file_extract)
runpy.run_path(auto_file_transform)
runpy.run_path(auto_file_load)
runpy.run_path(auto_file_olap)
