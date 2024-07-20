#extract.py

import os
from pyspark.sql import SparkSession
from config import directory_path


def extract(_spark: SparkSession, _delimiter: str):
    
    sources = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
    
    dataframes = {}
    
    for file in sources:
        file_path = os.path.join(directory_path, file)
        df_name = os.path.splitext(file)[0]
        dataframes[df_name] = _spark.read.format("CSV") \
            .options(header = True, inferSchema = True, delimiter = _delimiter, encoding = "UTF-8") \
            .load(file_path)
        
    return dataframes
