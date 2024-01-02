## Automated Data Preprocessing Framework: Property Scraper
### Execute run.py to automate all these process:
1. Extraction: extract.py 
2. Transformation: transform.py in 
3. Load: staging_pgsql_iproperty.py 
4. OLAP Transformation: pgsql_olap_iproperty.py
  
Run the following command in the terminal to create a python virtual env and install python libraries from the requirements text file:
python -m venv prj_venv
source prj_venv/Scripts/activate
python -m pip install -r requirements.txt

### Things-to-do:
- Add logic to switch into linux/win/mac script version
- Improvise logging for each script
- Refactor to OOP
- Add logging to each function and class
- Migrate python to golang in separate repo
  
## Property Data Analysis: Dashboard Application
### Deploy (Linux server) using command: gunicorn --worker-tmp-dir /dev/shm dashboard:server
5. Flask/Dash Web Server: dashboard.py
  
### Things-to-do:
- Fix the callback functions for no.5
- Fix the session and state logic for no.5
- Move src code into docker container
- Build and deploy with docker
- Add navigation bar on the left side screen
- Add navigation bar logic to minimize into hamburger menu when in mobile state
  
## .env file schema:
pgsqlHost="your host"
pgsqlPort=pgsql port number
pgsqlUsername="your username"
pgsqlPassword="your password"
pgsqlDatabase="your databasename"
WEBURL1='https://www.iproperty.com.my/sale/kuala-lumpur/all-residential'
WEBURL2='https://www.iproperty.com.my/sale/selangor/all-residential'
WEBURL3='https://www.iproperty.com.my/sale/johor/all-residential'
WEBURL4='https://www.iproperty.com.my/sale/penang/all-residential'
WEBURL5='https://www.iproperty.com.my/sale/perak/all-residential'
WEBURL6='https://www.iproperty.com.my/sale/negeri-sembilan/all-residential'
WEBURL7='https://www.iproperty.com.my/sale/melaka/all-residential'
WEBURL8='https://www.iproperty.com.my/sale/pahang/all-residential'
WEBURL9='https://www.iproperty.com.my/sale/sabah/all-residential'
WEBURL10='https://www.iproperty.com.my/sale/sarawak/all-residential'
WEBURL11='https://www.iproperty.com.my/sale/kedah/all-residential'
WEBURL12='https://www.iproperty.com.my/sale/putrajaya/all-residential'
WEBURL13='https://www.iproperty.com.my/sale/kelantan/all-residential'
WEBURL14='https://www.iproperty.com.my/sale/terengganu/all-residential'
WEBURL15='https://www.iproperty.com.my/sale/perlis/all-residential'
WEBURL16='https://www.iproperty.com.my/sale/labuan/all-residential'
MAIN_DIR="your project full path"
CHROME_DRIVER="your chromedriver filename, must include .exe" # if you update your chrome browser to latest version, then need to change to a new driver version.
CHROME_DRIVER_LINUX="chromedriver-headless-linux64-120-0-6099-109" # DON'T USE IT YET
CONFIG_DIR="config"
LOG_DIR="logs"
RAW_DIR="your project full path then concat with \data\\raw"
STAGING_DIR="your project full path then concat with \data\\staging"
SCHEMA_DIR="schema"
DATABASE_DIR="src\database"
PY_ENV="prj_venv\Scripts\python"
AUTO_DIR="auto"
KEY_SCHEMA_RAW="raw_iproperty"
KEY_SCHEMA_STAGING="staging_iproperty"
KEY_SCHEMA_OLAP="olap_iproperty"
EXTRACT="your project full path then concat with \src\01_extract"
TRANSFORM="your project full path then concat with \src\02_transform"
LOAD="your project full path then concat with \src\03_load"
OLAP="your project full path then concat with \src\04_olap"

