# Automated Preprocessing Framework for Housing Price Analysis

Process Flow:
1. Execute extract.py in 01_extract
2. Execute transform.py in 02_transform
3. Execute staging_pgsql_iproperty.py in 03_load
4. Execute pgsql_olap_iproperty.py in 04_olap (optional)
5. Execute app.py in app
6. Execute dashboard.py in app

gunicorn --worker-tmp-dir /dev/shm dashboard:server
