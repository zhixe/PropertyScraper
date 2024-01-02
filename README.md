## Automated Preprocessing Framework: Property Scraper
1. Execute extract.py in 01_extract
2. Execute transform.py in 02_transform
3. Execute staging_pgsql_iproperty.py in 03_load
4. Execute pgsql_olap_iproperty.py in 04_olap (optional)

## Property Data Analysis App
5. Execute dashboard.py in app

gunicorn --worker-tmp-dir /dev/shm dashboard:server
