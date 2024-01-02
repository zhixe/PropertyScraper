## Automated Data Preprocessing Framework: Property Scraper
### Execute run.py to automate all these process:
1. Extraction: extract.py 
2. Transformation: transform.py in 
3. Load: staging_pgsql_iproperty.py 
4. OLAP Transformation: pgsql_olap_iproperty.py
  
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
