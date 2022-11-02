# Data Engineering - Building a Data Pipeline with Machine Learning Models for Mobile Food Facilities in San Francisco, USA

This projects aims to create a DAG on Apache Airflow for the ETL process in collecting Mobile Food Facilities data from DataSF and Yelp Fusion API.

On faculty, airflow will work when Jupyter Notebook is shutdown, and Airflow webserver configurated to -p8888.

There is only one Python script that contains the whole DAG with ETL process, please ensure the file is within the "dags" folder under airflow_home, when running the DAG.

Other notes:

1. install_scratch.sh installs the relevants jars, java, PostgreSQL, Spark and Apache-airflow.

2. To install requirements, run 'pip install -r requirements.txt' in CLI
