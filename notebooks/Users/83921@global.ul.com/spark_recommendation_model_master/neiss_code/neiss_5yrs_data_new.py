# Databricks notebook source
def map_code(item, code_map):
    item['_source']['Fire_Involvement']=code_map['fire'][item['_source']['Fire_Involvement']]
    item['_source']['Body_Part']=code_map['bodypart'][item['_source']['Body_Part']]
    item['_source']['Disposition']=code_map['disposition'][item['_source']['Disposition']]
    item['_source']['Product_1']=code_map['product_code'][item['_source']['Product_1']]
    item['_source']['Product_2']=code_map['product_code'][item['_source']['Product_2']] if item['_source']['Product_2']not in ['0','',None] else None
    item['_source']['Diagnosis']=code_map['diagnosis'][item['_source']['Diagnosis']]
    return item


# COMMAND ----------

def download_data(code_map):
    """
    Download data and return as Pandas DF
    """
    es_conn = Elasticsearch(
        ["https://reader:reader@opendata.ul.com/api/es/"], )
    results = scan(es_conn, index="ppe_cpsc_neiss", size=10000,
                   query={
                       "query": {
                           "range": {
                               "@timestamp": {
                                   "gte": "now-5y/y",
                                   "lte":  "now/y"
                               }
                           }
                       }
                   },)
    report_id=list() 
    fire=list() 
    bodypart=list() 
    disposition=list() 
    product_1=list()
    product_2=list()
    diagnosis=list()
    narrative_1=list()
    narrative_2=list()
    def clean_text(item):
        if type(item) == type({'ds':1}):
            print(item)
        if item not in ['0','',None,{}]:
            return str(item).replace(',',' ')
        else:
            return None
    for item in results:
        print(item)
        item = map_code(item, code_map)
        report_id.append(item['_id'])
        fire.append(clean_text(item['_source'].get('Fire_Involvement', {})))
        bodypart.append(clean_text(item['_source'].get('Body_Part', {})))
        disposition.append(clean_text(item['_source'].get('Disposition', {})))
        product_1.append(clean_text(item['_source'].get('Product_1', {})))
        product_2.append(clean_text(item['_source'].get('Product_2', {})))
        diagnosis.append(clean_text(item['_source'].get('Diagnosis', {})))
        narrative_1.append(clean_text(item['_source'].get('Narrative_1', {})))
        narrative_2.append(clean_text(item['_source'].get('Narrative_2', {})))

            
        return pd.DataFrame(list(zip(report_id, fire, bodypart, disposition, product_1,product_2,diagnosis,narrative_1,narrative_2)),columns=['id','fire','bodypart','disposition','product_1','product_2','diagnosis','narrative_1','narrative_2'])
        

# COMMAND ----------

import requests
import numpy as np
import pandas as pd

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from datetime import datetime, timedelta
from pyspark.sql.types import *

#create schema
neiss_schema = StructType([StructField("id", StringType(), True),StructField("fire", StringType(), True),StructField("bodypart", StringType(), True),StructField("disposition", StringType(), True),StructField("product_1", StringType(), True),StructField("product_2", StringType(), True),StructField("diagnosis", StringType(), True),StructField("narrative_1", StringType(), True),StructField("narrative_2", StringType(), True)])

data_url = 'https://neiss.blob.core.windows.net/neiss/neiss_code.json'
code_map_data = requests.get(data_url) # create HTTP response object 
code_map = code_map_data.json()
#print(code_map)
results = download_data(code_map)
output_df = spark.createDataFrame(results,neiss_schema)
output_df.show()
output_df.write.option("header","true").mode("overwrite").csv("mnt/neiss/data_neiss.csv")

# COMMAND ----------

df=spark.read.csv("mnt/neiss/data_neiss.csv")
df.show()

# COMMAND ----------

