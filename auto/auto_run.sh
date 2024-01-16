#!/bin/bash
set -e

cd /root/projects/PropertyScraper/prj_venv
source bin/activate
cd /root/projects/PropertyScraper/src/01_extract_linux-64
python3 extract.py

cd /root/projects/PropertyScraper/src/02_transform
python3 transform.py

cd /root/projects/PropertyScraper/src/03_load
python3 staging_pgsql_iproperty.py

cd -
deactivate
trap 'echo "An error occurred. Exiting the program.."; exit 1' ERR
