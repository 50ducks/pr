#!/usr/bin/env python
# coding: utf-8

# In[1]:


import psycopg

from pyspark.sql import SparkSession, DataFrame
from datetime import datetime

import logs

from extract import extract
from transform import to_date_format, deduplication, clean_dataframe
from load import load_data
from config import db_schema, properties, url, logs_table


# In[2]:


#starting SparkSession

appName = 'Jupyter'
master = 'local'
spark = SparkSession \
.builder \
.appName(appName) \
.master(master) \
.config("spark.sql.caseSensitive", "false") \
.config("spark.jars", "postgresql-42.7.2.jar") \
.getOrCreate()

logs.set_start_time(datetime.now())


# In[3]:


#DB connection

conn = psycopg.connect(
        dbname="project",
        user=properties["user"],
        password=properties["password"],
        host="localhost",
        port="5432"
    )


# In[4]:


#extract

dataframes = extract(spark, ";")


# In[5]:


#transform

for name, df in dataframes.items():
    dataframes[name] = deduplication(df, db_schema + name)
    dataframes[name] = to_date_format(dataframes[name])
    dataframes[name] = clean_dataframe(dataframes[name])


# In[6]:


#load

for name, df in dataframes.items():
    load_data(spark, df, db_schema + name, conn)

logs.set_end_time(datetime.now())


# In[7]:


#logs

df = spark.sql(f"""
    SELECT * FROM values 
    (
        CAST('{logs.get_start_time()}' AS timestamp),
        CAST('{logs.get_end_time()}' AS timestamp),
        {logs.get_total_new_rows()},
        {logs.get_total_updated_rows()}
    ) AS (
        start_time, end_time, new_rows_count, updated_rows_count
    )
""")


df.show()

df.write.jdbc(url=url, table=logs_table, mode="append", properties=properties)


# In[8]:


#close connection and stop SparkSession

conn.close()
spark.stop()


# In[ ]:




