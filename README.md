# a) Automated Data Preprocessing Framework: Property Scraper
## Execute run-win.py/run-linux.py to automate all these process:
### 1. Extraction: extract.py 
### 2. Transformation: transform.py in 
### 3. Load: staging_pgsql_iproperty.py 
### 4. Advanced Transformation: pgsql_olap_iproperty.py

# Things-to-do:
## - Move into docker container
## - Build and deploy in docker
## - Fix the callback functions for no.5



# b) Property Data Analysis Dashboard Application
## Deploy (Linux server) using command: gunicorn --worker-tmp-dir /dev/shm dashboard:server
### 5. Flask/Dash Web Server: dashboard.py

# Things-to-do:
## - Move into docker container
## - Build and deploy in docker
## - Fix the callback functions for no.5