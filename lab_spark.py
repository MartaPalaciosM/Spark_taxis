import findspark
findspark
from pyspark.sql import SparkSession

#we are going to crate a SparkSession with the name "lab_spark"
spark = SparkSession.builder.appName("lab_spark").getOrCreate()

#obtain the SparkContext from the SparkSession
spark_context = spark.sparkContext

#libraries
import pandas as pd
import numpy as np
import time
#import pickle

#import functions and classes from pyspark.sql
from pyspark.sql.functions import col, round, expr, from_unixtime, unix_timestamp, date_format #module to work with columns
from pyspark.sql.types import IntegerType #module to work with integer data types

def spark_task(file_url, taxi_rdd)
    
    #we need to calculate the time it takes to read the data
    start_time = time.time()
    #permissive mode allows to load the data even if there are missing values
    taxi_data_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").option("mode", "PERMISSIVE").load(file_url) 

    
    end_time = time.time()
    time_to_read.append(end_time - start_time)#time it takes to read the data using append to add the time to the list


    #cleaning data
    start_time = time.time()
    taxi_data_df = taxi_data_df.withColumn("trip_time_hours", round((unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 3600, 2)) #calculate the trip time in hours

    #filter values of tips and other taxes lower than 0
    for column in ["tip_amount", "tolls_amount", "improvement_surcharge", "total_amount"]:
        taxi_data_df = taxi_data_df.filter(col(column) >= 0)
    
    #filter trips of negative duration
    taxi_data_df = taxi_data_df.filter(col("trip_time_hours") >= 0)

    #filter invalid payment
    #taxi_data_df = taxi_data_df.filter(taxi_data_df[column]>0)

    end_time = time.time()
    time_data_cleaning.append(end_time - start_time)#time it takes to clean the data using append to add the time to the list
