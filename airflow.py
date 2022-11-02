# Necessary Packages
import numpy as np
import pandas as pd
import json
import time
import concurrent.futures
from sodapy import Socrata
import matplotlib.pyplot as plt
import seaborn as sns
from subprocess import call
import requests
import pickle # data processing, pickle file I/O
import json # data processing, json file I/O
import random # data quality testing
import sqlalchemy # copy pd dataframe to Redshift 
from sqlalchemy.types import * # load staging_tables
import warnings
from itertools import product
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Sourcing
import requests
import urllib.request
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import itertools
import concurrent.futures

# formats
import json

# time
import datetime
import time
import pendulum

# S3 and postgres
import psycopg2 
import io
from io import StringIO, BytesIO
import boto3

# Spark
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/project/spark-3.2.1-bin-hadoop3.2"
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql import SQLContext
import pyarrow.parquet as pq
import psycopg2

#DAG
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow import DAG
from datetime import datetime


# == api_function.py

API_key = Socrata("data.sfgov.org", None) # API Key 


def extract_mobile_food_schedule():
    '''
    This function extracts the data of Mobile Food Facilities schedule in San Francisco
    '''
    API_key = Socrata("data.sfgov.org", None)
    mobile_food_schedule = API_key.get("#API_key", limit=2000)
    return pd.DataFrame(mobile_food_schedule)
    
def extract_mobile_food_permit():
    '''
    This function extracts the data of Mobile Food Facilities schedule in San Francisco
    '''
    API_key = Socrata("data.sfgov.org", None)
    mobile_food_permit = API_key.get("#API_key", limit=2000)
    return pd.DataFrame(mobile_food_permit)
    
def Tuples(dataframe):
    tuples = dataframe[["applicant","location"]].apply(tuple, axis=1)
    tuples = list(dict.fromkeys(tuples))
    return tuples

def yelp_df (tuples):
    url = "https://api.yelp.com/v3/businesses/search"
    key = ##Your API KEY
    header = {'Authorization':'Bearer %s' % key}
    for name, location in tuples:
        search_parameter = {
            'location': location + 'San Francisco',
            'term':name,
            'limit':50,
#            'offset':offset,
        }
        response = requests.get(url, headers=header, params = search_parameter)
        raw_data = response.json()
    return raw_data
        
def into_df_yelp(raw_data):
    listings = []
    cols = ['Yelp_id','Name', 'Review', 'Rating', 'Address','Phone', 'Latitude','Longitude']

    for business in raw_data['businesses']:
        yelp_id = business['id']
        name = business['name']
        review = business['review_count']
        rating = business['rating']
        address = business['location']['display_address'][0]
        phone = business['display_phone']
        lat = business['coordinates']['latitude']
        long1 = business['coordinates']['longitude']
        
        
        listings.append([yelp_id, name, review, rating, address, phone, lat, long1])
        df2 = pd.DataFrame.from_records(listings, index='Name', columns=cols)
    return df2
    
def extract_yelp(schedule_df1):
    tuples = Tuples(schedule_df1)
    raw_data = yelp_df(tuples)
    df2 = into_df_yelp(raw_data)
    df2.reset_index(inplace=True)
    return df2


# == Extract_elt.py
def extract_permit():
    '''

    This function extracts the data of Mobile Food Facilities Permits in San Francisco
    
    '''
    mobile_food_permit_df = extract_mobile_food_permit()
    return mobile_food_permit_df

def extract_schedule():
    '''

    This function extracts the data of Mobile Food Facilities Permits in San Francisco
    
    '''
    mobile_food_schedule_df = extract_mobile_food_schedule()
    return mobile_food_schedule_df   
    


    
## == transformation_elt.py

def transformed_schedule_df(schedule_df):
    schedule_df["schedule_id"] = range(0, len(schedule_df))
    return schedule_df

def transformed_permit_df(permit_df):
    t_permit_df = permit_df.drop(columns=[":@computed_region_yftq_j783", ":@computed_region_p5aj_wyqh", ":@computed_region_rxqg_mtj9", ":@computed_region_bh8s_q3mv", ":@computed_region_fyvs_ahh9"])
    return t_permit_df
    
def transformed_permit_df2(permit_df):
    permit = permit_df
    permit_df2 = pd.DataFrame(np.repeat(permit.values, 2, axis=0))
    permit_df2.columns = permit.columns
    return permit_df2
    
def details(permit_df):
    permit = permit_df
    df1 = permit[["permit","applicant", "facilitytype"]]
    df1 = df1.drop_duplicates('permit')
    return df1     
    
def premitted_biz_details(schedule_df):
    schedule = schedule_df
    df2 = schedule_df[["schedule_id","permit"]]
    return df2
    
def applicant_details(permit_df):
    permit = permit_df
    df7 = permit[["objectid","permit"]]
    return df7
    
def location(schedule_df, permit_df):
    schedule = schedule_df
    permit = permit_df
    df5 = schedule[["schedule_id","permit", "location","locationdesc","locationid","cnn", "block"]] #, "lot", "x", "y", "latitude", "longitude"]]
    df6 = permit[["permit", "cnn", "address"]]
    merge_location = pd.merge(df5, df6, how='outer', left_on= ['permit','cnn'], right_on=['permit','cnn'])
    merge_location1 = merge_location.drop(columns=['permit'])
    merge_location1 = merge_location1.sort_values('schedule_id', ascending=True).drop_duplicates('schedule_id')
    merge_location1 = merge_location1[merge_location1['schedule_id'].notna()]
#    merge_location1["latitude"] = merge_location1["latitude"].round(6)
#    merge_location1["longitude"] = merge_location1["longitude"].round(6)
#    merge_location1["x"] = merge_location1["x"].round(3)
#    merge_location1["y"] = merge_location1["y"].round(3)
    return merge_location1 
    
def food_type(schedule_df, permit_df):
    schedule = schedule_df
    permit = permit_df
    df3 = schedule[["permit","applicant","optionaltext","coldtruck"]]
    df4 = permit[["permit","fooditems"]]
    merge_food_type = pd.merge(df3, df4, on="permit")
    merge_food_type = merge_food_type.drop_duplicates('permit')
    return merge_food_type   
    
def permit_motification_details(schedule_df):
    motification_details = schedule_df
    motification_details = motification_details[["schedule_id", "addr_date_create", "addr_date_modified"]]
    motification_details['1'] = pd.to_datetime(motification_details['addr_date_create']).dt.strftime('%Y-%m-%d')
    motification_details['2'] = pd.to_datetime(motification_details['addr_date_modified']).dt.strftime('%Y-%m-%d')
    motification_details = motification_details.drop(columns = ["addr_date_create", "addr_date_modified"])
    motification_details = motification_details.rename(columns={"1": "addr_date_create", "2": "addr_date_modified"})
    return motification_details  
    

def permit_application_details(permit_df):
    application_details = permit_df
    application_details = application_details[["objectid", "status", "approved", "received", "priorpermit", "expirationdate"]]
    return application_details   
    
def operating_day(schedule_df):
    day = schedule_df
    day = day[["schedule_id","dayorder","dayofweekstr"]]
    return day    
    
def operation_time(schedule_df):
    operation = schedule_df
    operation = operation[["schedule_id","starttime","endtime","start24","end24"]]
    return operation

def yelp_info(schedule_df,yelp_df_final_1):
    schedule = schedule_df
    yelp = yelp_df_final_1
    schedule = schedule["applicant"]
    yelp = yelp[["Name","Yelp_id","Review","Rating","Address", "Phone", "Latitude", "Longitude"]]
    df3 = pd.merge(schedule, yelp,  how='left', left_on=['applicant'], right_on = ['Name'])
    df3 = df3.drop(columns=['applicant'])
    df3 = df3.drop_duplicates()
    df3 = df3.dropna()
    return df3
    
def num_permit_per_business(schedule_df):
    schedule = schedule_df
    df10 = schedule[["permit","applicant"]]
    df10["no_permit_owned_per_business"] = df10.groupby('applicant')['applicant'].transform('count')
    df10 = df10.drop_duplicates(subset=['applicant'])
    return df10
  
# == Load1.py


def Load1(): 
    
    def df_to_tuple(df):
        tup = list(df.itertuples(index=False, name=None))
        return tup
    
    REGION = 'us-east-1'
    ACCESS_KEY_ID = ##Your ACCESS_KEY_ID
    SECRET_ACCESS_KEY = ##Your ACCESS_KEY_ID
    BUCKET_NAME = 'de-individual-2022'
    s3csv = boto3.client('s3', 
     region_name = REGION,
     aws_access_key_id = ACCESS_KEY_ID,
     aws_secret_access_key = SECRET_ACCESS_KEY
     )
     
         # We use boto3 to take data from S3 that will be uploaded into Postgres (and later into parquet format in s3 
    obj1 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'transform/permit_motification_details_table.csv')
    obj2 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'transform/operation_time_table.csv')
    obj3 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'transform/permit_application_details_table.csv')
    obj4 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'transform/operating_day_table.csv')
    obj5 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'transform/details_table.csv')
    obj6 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'transform/food_type_table.csv')
    obj7 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'transform/location_table.csv')
    obj8 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'transform/premitted_biz_details_table.csv')
    obj9 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'transform/applicant_details_table.csv')
    obj10 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'transform/yelp_info_table.csv')
    obj11 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'transform/num_permit_per_business_table.csv')
    
    
    permit_motification_details_table = pd.read_csv(io.BytesIO(obj1['Body'].read()), encoding='utf8')
    operation_time_table = pd.read_csv(io.BytesIO(obj2['Body'].read()), encoding='utf8')
    permit_application_details_table = pd.read_csv(io.BytesIO(obj3['Body'].read()), encoding='utf8')
    operating_day_table = pd.read_csv(io.BytesIO(obj4['Body'].read()), encoding='utf8')
    details_table = pd.read_csv(io.BytesIO(obj5['Body'].read()), encoding='utf8')
    food_type_table = pd.read_csv(io.BytesIO(obj6['Body'].read()), encoding='utf8')
    location_table = pd.read_csv(io.BytesIO(obj7['Body'].read()), encoding='utf8')
    premitted_biz_details_table = pd.read_csv(io.BytesIO(obj8['Body'].read()), encoding='utf8')
    applicant_details_table = pd.read_csv(io.BytesIO(obj9['Body'].read()), encoding='utf8')
    yelp_info_table = pd.read_csv(io.BytesIO(obj10['Body'].read()), encoding='utf8')
    num_permit_per_business_table = pd.read_csv(io.BytesIO(obj11['Body'].read()), encoding='utf8')
   
   
    conn = psycopg2.connect(
       dbname ="database_1", user='de_individual', password=##Your password
       , host=#your host,
       port= '5432'
    )
    conn.autocommit = True
    
    cursor = conn.cursor()
    
    cursor.execute("DROP SCHEMA IF EXISTS pp_schema CASCADE")
    cursor.execute("CREATE SCHEMA pp_schema")
    
    # To ensure no technical issues are encountered, we delete the tables for now, should they already exist. 

    cursor.execute("DROP TABLE IF EXISTS pp_schema.permit_motification_details CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.operation_time CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.permit_application_details CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.operating_day CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.details CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.food_type CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.location CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.premitted_biz_details CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.applicant_details CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.yelp_info CASCADE")
    cursor.execute("DROP TABLE IF EXISTS pp_schema.num_permit_per_business CASCADE")
    
    # Define the strings that will be used as queries to create tables
    
   
    details = """
            CREATE TABLE pp_schema.details (
            permit varchar PRIMARY KEY,   
            applicant varchar,
            facilitytype varchar
            )
            """
            
    premitted_biz_details = """
            CREATE TABLE pp_schema.premitted_biz_details (
            schedule_id int PRIMARY KEY,
            permit varchar not null references pp_schema.food_type("permit")
            )
            """
            
    applicant_details = """
            CREATE TABLE pp_schema.applicant_details (
            objectid int PRIMARY KEY,
            permit varchar not null references pp_schema.details("permit")
            )
            """

    location = """
            CREATE TABLE pp_schema.location (
            schedule_id int PRIMARY KEY not null references pp_schema.premitted_biz_details("schedule_id"),
            location varchar,
            locationdesc varchar,
            locationid bigint,
            cnn bigint, 
            block varchar,
            lot varchar,
            address varchar
            )
            """

            
    food_type = """
            CREATE TABLE pp_schema.food_type (
            permit varchar PRIMARY KEY not null references pp_schema.details("permit"),
            applicant varchar,
            optional varchar,
            coldtruck varchar,
            fooditems varchar
            )
            """
    
    permit_motification_details = """
            CREATE TABLE pp_schema.permit_motification_details (
            schedule_id int PRIMARY KEY not null references pp_schema.premitted_biz_details("schedule_id"),
            addr_date_create varchar,
            addr_date_modified varchar
            )
            """
            
    operation_time = """
            CREATE TABLE pp_schema.operation_time (
            schedule_id int PRIMARY KEY not null references pp_schema.premitted_biz_details("schedule_id"),
            starttime varchar,
            endtime varchar,
            start24 varchar,
            end24 varchar
            )
            """
            
    permit_application_details = """
            CREATE TABLE pp_schema.permit_application_details (
            objectid int PRIMARY KEY not null references pp_schema.applicant_details("objectid"),
            status varchar,
            approved varchar,
            received varchar,
            priorpermit int,
            expirationdate varchar
            )
            """
    
    operating_day = """
            CREATE TABLE pp_schema.operating_day (
            schedule_id int PRIMARY KEY not null references pp_schema.premitted_biz_details("schedule_id"),
            dayorder int,
            dayofweekstr varchar
            )
            """
    
    num_permit_per_business = """
            CREATE TABLE pp_schema.num_permit_per_business (
            applicant varchar PRIMARY KEY,
            permit varchar not null references pp_schema.food_type("permit"),
            no_permit_owned_per_business int
            )
            """
            
    yelp_info = """
            CREATE TABLE pp_schema.yelp_info (
            Yelp_id varchar PRIMARY KEY,
            Name varchar not null references pp_schema.num_permit_per_business("applicant"),
            Review int,
            Rating int,
            Address varchar,
            Phone varchar,
            Latitude DECIMAL(8,6),
            Longitude DECIMAL(9,6)
            )
            """
            

    cursor.execute(details)
    cursor.execute(food_type)
    cursor.execute(applicant_details)
    cursor.execute(premitted_biz_details)
    cursor.execute(operation_time)
    cursor.execute(permit_application_details)
    cursor.execute(operating_day)
    cursor.execute(permit_motification_details)
    cursor.execute(num_permit_per_business)
    cursor.execute(location)
    cursor.execute(yelp_info)

    
    # Turn the tables into a list of tuples that can be ingested by the defined tables in PostgreSQL
    details = df_to_tuple(details_table)
    location = df_to_tuple(location_table)
    food_type = df_to_tuple(food_type_table)
    permit_motification_details = df_to_tuple(permit_motification_details_table)
    operation_time = df_to_tuple(operation_time_table)
    permit_application_details = df_to_tuple(permit_application_details_table)
    operating_day = df_to_tuple(operating_day_table)
    premitted_biz_details = df_to_tuple(premitted_biz_details_table)
    applicant_details = df_to_tuple(applicant_details_table)
    yelp_info = df_to_tuple(yelp_info_table)
    num_permit_per_business = df_to_tuple(num_permit_per_business_table)
    
    # Put the tables into PostgreSQL tables
    for i in details:
         cursor.execute("INSERT into pp_schema.details(permit, applicant, facilitytype) VALUES (%s, %s, %s)", i)
    print("List has been inserted to details table")
    
    
    for i in food_type:
        cursor.execute("INSERT into pp_schema.food_type(permit, applicant, optional, coldtruck, fooditems) VALUES (%s, %s, %s, %s, %s)", i)
    print("List has been inserted to food_type table")
    
    for i in premitted_biz_details:
        cursor.execute("INSERT into pp_schema.premitted_biz_details(schedule_id, permit) VALUES (%s, %s)", i)
    print("List has been inserted to premitted_biz_details table")
    

    for i in applicant_details:
        cursor.execute("INSERT into pp_schema.applicant_details(objectid, permit) VALUES (%s, %s)", i)
    print("List has been inserted to applicant_details table")
    
    
    for i in num_permit_per_business:
        cursor.execute("INSERT into pp_schema.num_permit_per_business(permit, applicant, no_permit_owned_per_business) VALUES (%s, %s, %s)", i)
    print("List has been inserted to num_permit_per_business table")
    
    
    for i in yelp_info:
        cursor.execute("INSERT into pp_schema.yelp_info(Name, Yelp_id, Review, Rating, Address, Phone, Latitude, Longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", i)
    print("List has been inserted to yelp_info table")
    
    
    for i in permit_motification_details:
        cursor.execute("INSERT into pp_schema.permit_motification_details(schedule_id, addr_date_create, addr_date_modified) VALUES (%s, %s, %s)", i)
    print("List has been inserted to permit_motification_details table")
    
    
    for i in operation_time:
        cursor.execute("INSERT into pp_schema.operation_time(schedule_id, starttime, endtime, start24, end24) VALUES (%s, %s, %s, %s, %s)", i)
    print("List has been inserted to operation_time table")
    
    
    for i in permit_application_details:
        cursor.execute("INSERT into pp_schema.permit_application_details(objectid, status, approved, received, priorpermit, expirationdate) VALUES (%s, %s, %s, %s, %s, %s)", i)
    print("List has been inserted to permit_application_details table")
    
    
    for i in operating_day:
        cursor.execute("INSERT into pp_schema.operating_day(schedule_id, dayorder, dayofweekstr) VALUES (%s, %s, %s)", i)
    print("List has been inserted to operating_day table")
    

    

    # commit the changes to the database
    conn.commit()
    conn.close()


## == Spark_elt.py 


def Spark_ETL():
    '''
    
    Using spark to get data from postgres, do quick analysis, store the analysis (graphs) into S3.
    Will then store the data from Postgres into S3 as parquet files to be used later. 
    
    '''
    spark = SparkSession \
        .builder \
        .appName("PySpark App") \
        .config("spark.jars", "/project/postgresql-42.3.2.jar") \
        .getOrCreate()
    
    sqlContext = SQLContext(spark)
    
    postgres_uri = #your_URI
    user = "de_individual"
    password = #your_password
    details = "pp_schema.details"
    food_type = "pp_schema.food_type"
    permit_motification_details = "pp_schema.permit_motification_details"
    operation_time = "pp_schema.operation_time"
    permit_application_details = "pp_schema.permit_application_details"
    operating_day = "pp_schema.operating_day"
    premitted_biz_details = "pp_schema.premitted_biz_details"
    applicant_details = "pp_schema.applicant_details"
    yelp_info = "pp_schema.yelp_info"
    num_permit_per_business = "pp_schema.num_permit_per_business"
   
    
    df_details = spark.read \
        .format("jdbc") \
        .option("url", postgres_uri) \
        .option("dbtable", details) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()


    df_food_type = spark.read \
        .format("jdbc") \
        .option("url", postgres_uri) \
        .option("dbtable", food_type) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    df_permit_motification_details = spark.read \
        .format("jdbc") \
        .option("url", postgres_uri) \
        .option("dbtable", permit_motification_details) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    df_operation_time = spark.read \
        .format("jdbc") \
        .option("url", postgres_uri) \
        .option("dbtable", operation_time) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    df_permit_application_details = spark.read \
        .format("jdbc") \
        .option("url", postgres_uri) \
        .option("dbtable", permit_application_details) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    df_operating_day = spark.read \
        .format("jdbc") \
        .option("url", postgres_uri) \
        .option("dbtable", operating_day) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
        
    df_premitted_biz_details = spark.read \
        .format("jdbc") \
        .option("url", postgres_uri) \
        .option("dbtable", premitted_biz_details) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
        
    df_applicant_details = spark.read \
        .format("jdbc") \
        .option("url", postgres_uri) \
        .option("dbtable", applicant_details) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
        
    df_yelp_info = spark.read \
        .format("jdbc") \
        .option("url", postgres_uri) \
        .option("dbtable", yelp_info) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
        
    df_num_permit_per_business = spark.read \
        .format("jdbc") \
        .option("url", postgres_uri) \
        .option("dbtable", num_permit_per_business) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    

    df_details.createTempView("details")
    df_food_type.createTempView("food_type")
    df_permit_motification_details.createTempView("permit_motification_details")
    df_operation_time.createTempView("operation_time")
    df_permit_application_details.createTempView("permit_application_details")
    df_operating_day.createTempView("operating_day")
    df_premitted_biz_details.createTempView("premitted_biz_details")
    df_applicant_details.createTempView("applicant_details")
    df_num_permit_per_business.createTempView("num_permit_per_business")
    df_yelp_info.createTempView("yelp_info")
    
    query1 = "SELECT DISTINCT num_permit_per_business.applicant, COUNT(premitted_biz_details.schedule_id) as schedule_count \
                FROM num_permit_per_business \
                LEFT JOIN premitted_biz_details \
                ON num_permit_per_business.permit = premitted_biz_details.permit \
                GROUP BY num_permit_per_business.applicant \
                ORDER BY schedule_count DESC \
                LIMIT 5"    
    
    
    REGION = 'us-east-1'
    ACCESS_KEY_ID = #your_Access_key_id
    SECRET_ACCESS_KEY = #your_secret_access_key
    BUCKET_NAME = 'de-individual-2022'
    

     
    s3img = boto3.client('s3', 
         region_name = REGION,
         aws_access_key_id = ACCESS_KEY_ID,
         aws_secret_access_key = SECRET_ACCESS_KEY
         )
         
    FileName1 = 'images/schedule_per_business.png'
    
    stats1 = spark.sql(query1).toPandas()
    total_pie = stats1.schedule_count.sum()
    values_pie = stats1.schedule_count.tolist()
    labels_pie = stats1.applicant.tolist()
    img_data1 = io.BytesIO()
    plt.figure(figsize=(7, 7))
    plt.pie(values_pie, labels=labels_pie, autopct='%.0f%%')
    plt.title('Top 5 Mobile Food Establishments with the highest share in approved schedule')
    plt.savefig(img_data1, format='png') # New
    img_data1.seek(0)
    response1=s3img.put_object(Body=img_data1,
                               Bucket=BUCKET_NAME, ContentType='image/png', 
                               Key=FileName1)
    
    
    REGION = 'us-east-1'
    ACCESS_KEY_ID = #your_Access_key_id
    SECRET_ACCESS_KEY = #your_secret_access_key
    BUCKET_NAME = 'de-individual-2022'
    
    s3parquet = boto3.client('s3', 
        region_name = REGION,
        aws_access_key_id = ACCESS_KEY_ID,
        aws_secret_access_key = SECRET_ACCESS_KEY
        )
    
    def store_in_S3(df, name):
        '''
        name is a string that will be given to the parquet file when placed in S3
        The files will be stored in a folder called parquets
        
        '''
        temp = df.toPandas()
        out_buffer=BytesIO()
        temp.to_parquet(out_buffer, index=False)
        s3parquet.put_object(Bucket=BUCKET_NAME, Key='parquets/' + name + '.parquet', Body=out_buffer.getvalue())
    
    store_in_S3(df_details, 'df_details')
    store_in_S3(df_food_type, 'df_food_type')
    store_in_S3(df_permit_motification_details, 'df_permit_motification_details')
    store_in_S3(df_operation_time, 'df_operation_time')
    store_in_S3(df_permit_application_details, 'df_permit_application_details')
    store_in_S3(df_operating_day, 'df_operating_day')
    store_in_S3(df_premitted_biz_details, 'df_premitted_biz_details')
    store_in_S3(df_applicant_details, 'df_applicant_details')
    store_in_S3(df_num_permit_per_business, 'df_num_permit_per_business')
    store_in_S3(df_yelp_info, 'df_yelp_info')

    spark.stop()


## DAG


# Setting up S3 bucket connection:
REGION = 'us-east-1'
ACCESS_KEY_ID = #your_Access_key
SECRET_ACCESS_KEY = #your_secret_access_key
BUCKET_NAME = 'de-individual-2022'
s3csv = boto3.client('s3', 
 region_name = REGION,
 aws_access_key_id = ACCESS_KEY_ID,
 aws_secret_access_key = SECRET_ACCESS_KEY
 )
 
 

# Saving csv files to s3
def save_csv_s3(df, folder, name):
    csv_buffer=StringIO()
    df.to_csv(csv_buffer, index=False)
    response=s3csv.put_object(Body=csv_buffer.getvalue(),
                           Bucket=BUCKET_NAME,
                           Key=folder + "/" + name + ".csv")
                           
def Extract():
    schedule_df = extract_schedule() # Make and store
    save_csv_s3(schedule_df, 'extract', 'schedule_df')
    permit_df = extract_permit()
    save_csv_s3(permit_df, 'extract', 'permit_df')
    yelp_df_final_1= extract_yelp(schedule_df)
    save_csv_s3(yelp_df_final_1, 'extract', 'yelp_df_final_1')
    
def Transform1():
    obj1 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'extract/schedule_df.csv')
    obj2 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'extract/permit_df.csv')
    obj3 = s3csv.get_object(Bucket= BUCKET_NAME , Key = 'extract/yelp_df_final_1.csv')
    schedule_df = pd.read_csv(io.BytesIO(obj1['Body'].read()), encoding='utf8')
    permit_df = pd.read_csv(io.BytesIO(obj2['Body'].read()), encoding='utf8')
    yelp_df_final = pd.read_csv(io.BytesIO(obj3['Body'].read()), encoding='utf8')
    
    schedule_df1 = transformed_schedule_df(schedule_df)
    permit_df1 = transformed_permit_df(permit_df)
    
    permit_ml = transformed_permit_df2(permit_df1)
    save_csv_s3(permit_ml, 'transform', 'permit_ml')
    
    #Get simple tables
    permit_motification_details_table = permit_motification_details(schedule_df1)
    operation_time_table = operation_time(schedule_df1)
    permit_application_details_table = permit_application_details(permit_df1)
    operating_day_table = operating_day(schedule_df1)
    details_table = details(permit_df1)
    food_type_table = food_type(schedule_df1, permit_df1)
    location_table = location(schedule_df1, permit_df1)
    premitted_biz_details_table = premitted_biz_details(schedule_df1)
    applicant_details_table = applicant_details(permit_df1)
    num_permit_per_business_table = num_permit_per_business(schedule_df1)
    yelp_info_table = yelp_info(schedule_df1,yelp_df_final)

    # Loading data into S3    
    save_csv_s3(permit_motification_details_table, 'transform', 'permit_motification_details_table')
    save_csv_s3(operation_time_table, 'transform', 'operation_time_table')
    save_csv_s3(permit_application_details_table, 'transform', 'permit_application_details_table')
    save_csv_s3(operating_day_table, 'transform', 'operating_day_table')
    save_csv_s3(details_table, 'transform', 'details_table')
    save_csv_s3(food_type_table, 'transform', 'food_type_table')
    save_csv_s3(location_table, 'transform', 'location_table')
    save_csv_s3(premitted_biz_details_table, 'transform', 'premitted_biz_details_table')
    save_csv_s3(applicant_details_table, 'transform', 'applicant_details_table')
    save_csv_s3(num_permit_per_business_table, 'transform', 'num_permit_per_business_table')
    save_csv_s3(yelp_info_table, 'transform', 'yelp_info_table')

# Loading Stage
def df_to_tuple(df):
    tup = list(df.itertuples(index=False, name=None))
    return tup
    

Load = Load1

Spark_func = Spark_ETL
    
# Starting the DAG Process

with DAG(
    dag_id='airflow_6',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description="ETL for mobile food establishments in San Francisco ",
    schedule_interval= '@weekly',
    start_date=pendulum.datetime(2022, 5, 2, tz="UTC"),
    catchup=False,
    tags=['airflow_6']) as dag:
        
    t1 = PythonOperator(
        task_id='Extract',
        python_callable=Extract,
    )
    
    t2 = PythonOperator(
        task_id='Transform',
        python_callable=Transform1,
    )
    

    t3 = PythonOperator(
        task_id='Load_PostgreSQL',
        python_callable=Load,
    )

    t4 = PythonOperator(
        task_id='Spark',
        python_callable=Spark_func,
    )

    t1 >>  t2 >> t3 >> t4 


    
