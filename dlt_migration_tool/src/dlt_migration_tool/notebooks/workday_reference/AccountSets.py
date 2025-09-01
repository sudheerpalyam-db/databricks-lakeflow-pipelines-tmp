# Databricks notebook source
# MAGIC %pip install xmltodict

# COMMAND ----------

#Importing all the required Python Packages
import copy
import os
import dataclasses
import json
import logging
import pyspark.sql
import pyspark.sql.types as T
#from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    MapType
)
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import from_utc_timestamp
from pyspark.sql.functions import to_timestamp
import time

# COMMAND ----------

#Enabling automatic schema evolution
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 

# COMMAND ----------

# Setting up environment name and landing path as a variable from environmental variables. The below have to be updated before deployment

dbutils.widgets.text(name="env", defaultValue="dev")
dbutils.widgets.text(name="workday_path", defaultValue="s3://staging-workday-apa-com-au-syda-737465331764")
env = dbutils.widgets.get("env")
landing_path=dbutils.widgets.get("workday_path")

#Declaring the Landing Path of the parquet File
landing_parquet_path=landing_path+'/cdata_files/AccountSets.parquet'
landing_new_path=landing_path+'/cdata_files/AccountSets/'
bronze_control_table=f"edp_bronze_{env}.workday.control_table"

#Control table variable
control_table='edp_silver_'+env+'.workday_ods.control_table'

#Declaring the bronze table details
bronze_table_name='account_sets'
bronze_table='edp_bronze_'+env+'.workday.'+bronze_table_name
bronze_columns='Account_Set_ID,Account_Set_Name,Chart_of_Accounts,Account_Set_Reference,Load_Datetime'

#Declaring the silver table details
silver_catalog='edp_silver_'+env
silver_schema='workday_ods'

silver_table1='account_sets'

silver_full_table1='edp_silver_'+env+'.workday_ods.'+silver_table1

silvermergekey1="Account_Set_ID"
silvermergekeycondition1="s.Account_Set_ID = t.Account_Set_ID"
silverfinalmergekeycondition1="s.merge_key = t.Account_Set_ID"
silvercolumns1='Account_Set_ID,Account_Set_Name,Chart_of_Accounts,__START_AT,__END_AT'


silvercolumns1_values='s.Account_Set_ID,s.Account_Set_Name,s.Chart_of_Accounts,s.__START_AT,s.__END_AT'


silvermatchcondition1='s.Account_Set_Name <> t.Account_Set_Name or s.Chart_of_Accounts <> t.Chart_of_Accounts'

#Making variables for the sub-silver tables
silver_table2='account_set_reference'
silver_full_table2='edp_silver_'+env+'.workday_ods.'+silver_table2
silvercolumns2='Account_Set_ID,Account_Set_Reference_WID'

# COMMAND ----------

#Defining the re-usable functions of reading and writing 

#Function to read a json file from S3 path into a DataFrame
def read_json(landing_json_path):
    df_json=spark.read.format('json') \
        .option('multiline','true') \
            .option('inferColumnTypes', 'true') \
                .load(landing_json_path)
    return df_json

#Function to read a parquet file from S3 path into a DataFrame
def read_parquet(landing_parquet_path):
    df_parquet=spark.read.format('parquet') \
        .option('multiline','true') \
            .option('inferColumnTypes', 'true') \
                .load(landing_parquet_path)
    return df_parquet

#Function to read a csv file from S3 path into a DataFrame
def read_csv(landing_json_path):
    df_csv=spark.read.format('csv') \
        .option('sep', ',') \
        .option('header','true') \
            .option('inferSchema', 'true') \
                    .option('multiline','true') \
                        .load(landing_json_path)
    return df_csv

#Function to read a delta table
def read_table(table):
    df=spark.read.format('delta') \
        .table(table)
    return df

#Function to update column names(replacing dots with underscores)
def update_df_column_names(df):
    updated_columns = (column.replace('_-_','_').replace('.','_').replace('__','_').replace('-','_') for column in df.columns)
    df_json_updated = df.toDF(*updated_columns)
    return df_json_updated

#Function to write data into a table using append mode
def writetable_append(df,table):
    df.write.format('delta') \
    .mode('append') \
        .option('mergeSchema','true') \
            .saveAsTable(table)

#Function to write data into a table using overwrite mode
def writetable_overwrite(df,table):
    df.write.format('delta') \
    .mode('overwrite') \
        .option('overwriteSchema','true') \
            .saveAsTable(table)


#Function to identify new records for Type-2 merge
def newrecords_type2(silver_catalog,silver_schema,silver_table,silver_full_table,silvermergekeycondition,silvermatchcondition,newdataset,silvermergekey):
    silver_table_info_schema=spark.sql(f"Select * from system.information_schema.tables where table_name='{silver_table}' and table_catalog='{silver_catalog}' and table_schema='{silver_schema}'").collect()
    if(len(silver_table_info_schema))>0:
        df=spark.sql(f"Select s.*, 'null' as merge_key from {newdataset} s inner join {silver_full_table} t on {silvermergekeycondition}  and ({silvermatchcondition}) and  t.__END_AT='null' UNION all Select *, {silvermergekey} as merge_key from {newdataset} ")
    else:
        df=spark.sql(f"Select *, {silvermergekey} as merge_key from {newdataset}")
    return df

#Function to perform a Type-2 merge
def merge_type2(silver_catalog,silver_schema,silver_table,silver_full_table,silverfinalmergekeycondition,silvermatchcondition,df_finaldataset,temp_df,silvercolumns,silvercolumns_values):
    if(len(spark.sql(f"Select * from system.information_schema.tables where table_name='{silver_table}' and table_catalog='{silver_catalog}' and table_schema='{silver_schema}'").collect()))>0:
        spark.sql(f"""MERGE INTO {silver_full_table} t USING {df_finaldataset} s ON {silverfinalmergekeycondition} and t.__END_AT='null' WHEN MATCHED and t.__END_AT='null' and {silvermatchcondition} THEN UPDATE SET t.__END_AT=s.__START_AT WHEN NOT MATCHED THEN INSERT({silvercolumns}) VALUES({silvercolumns_values})""")
    else:
        writetable_append(temp_df,silver_full_table)

# COMMAND ----------

#Picking the max file date form the bronze control table
bronze_max_loadtime=spark.sql(f"""Select max(load_datetime) from {bronze_control_table} where bronze_table='{bronze_table_name}'""").collect()[0][0]
#Sending the files which have arrived after the max date into an array to load into Bronze table
cdata_files_list=[]
cdata_loadtimes=[]
for files in dbutils.fs.ls(landing_new_path):
    if files.modificationTime>bronze_max_loadtime:
        cdata_loadtimes.append(files.modificationTime)
        cdata_files_list.append(files.path)
if len(cdata_loadtimes)>0:
    max_cdatafile_time=max(cdata_loadtimes)

# COMMAND ----------

#BronzeDataLoad
if len(cdata_files_list)>0:
    for filepath in cdata_files_list:
        df_parquet=read_parquet(filepath)
        df_parquet_updated = update_df_column_names(df_parquet)
        df_parquet_updated=df_parquet_updated.select([col(c).cast("string") for c in df_parquet_updated.columns])
        writetable_append(df_parquet_updated,bronze_table)
 
    #Writing the max CDATA load time into the control table
    spark.sql(f"Insert into {bronze_control_table}(bronze_table,load_datetime,updated_datetime) values ('{bronze_table_name}',{max_cdatafile_time},from_unixtime(unix_timestamp(from_utc_timestamp(current_timestamp(), 'Australia/Melbourne'), 'dd.MM.yyyy HH:mm:ss.SSS')))")

# COMMAND ----------

#Loading the silver main table with Type-2 upserts
if len(spark.sql(f"Select * from {bronze_table} where load_datetime>(Select max(load_datetime) from {control_table} where bronze_table='{bronze_table_name}') and load_datetime<=(Select max(load_datetime) from {bronze_table})").collect())>0:
    #Read the data into a Dataframe
    df1=spark.sql(f"Select * from {bronze_table} where load_datetime>(Select max(load_datetime) from {control_table} where bronze_table='{bronze_table_name}') and load_datetime<=(Select max(load_datetime) from {bronze_table})")
    bronzecolorder1=[]
    bronzecolorder1 = bronze_columns.split(",")
    df1=df1.select(bronzecolorder1)
    df1.createOrReplaceTempView('df1temptable1')
    #Populating df2 for the sub-silver table
    df2=spark.sql("Select Account_Set_ID,Account_Set_Reference,load_datetime from df1temptable1") 
    df1_temp1=spark.sql(f"Select * from (Select *, row_number() over(partition by {silvermergekey1} order by load_datetime desc ) as rn from df1temptable1) a   where a.rn=1")
    df1_temp2=df1_temp1.drop('rn')
    df1_temp2=df1_temp2.withColumnRenamed('load_datetime','__START_AT')
    df1_temp2=df1_temp2.withColumn('__END_AT',lit('null'))
    silvercolorder1=[]
    silvercolorder1 = silvercolumns1.split(",")
    df1_temp2=df1_temp2.select(silvercolorder1)
    df1_temp2.createOrReplaceTempView('df1temptable2')
    df1newdataset='df1temptable2'
    df1=newrecords_type2(silver_catalog,silver_schema,silver_table1,silver_full_table1,silvermergekeycondition1,silvermatchcondition1,df1newdataset,silvermergekey1)
    df1.createOrReplaceTempView('table1')
    df1_finaldataset='table1'
    merge_type2(silver_catalog,silver_schema,silver_table1,silver_full_table1,silverfinalmergekeycondition1,silvermatchcondition1,df1_finaldataset,df1_temp2,silvercolumns1,silvercolumns1_values)

# COMMAND ----------

# This is the UDF Code
import csv
import sys
import xmltodict
from datetime import datetime
from io import StringIO
from pyspark import Row
from pyspark.sql.functions import udf
from pyspark.sql import functions as F


def transform_element(element):
    """Applies some rules developed by ERP to best transform the Workday XML element for databricks"""

    if isinstance(element, dict):
        return transform_dict(element)
    elif isinstance(element, list):
        return [transform_element(item) for item in element]
    else:
        return element


def transform_dict(element):
    # ID elements from Workday get processed a special way
    if "ID" in element:
        id_elements = element.pop("ID")
        if not isinstance(id_elements, (dict, list)):
            return id_elements

        transformed = transform_id_elements(
            id_elements if isinstance(id_elements, list) else [id_elements]
        )
        element.update(transformed)

    # Special [name: value] representation in workday
    if "@Type" in element and "#text" in element:
        return {'Name': element['@Type'], 'Value': element["#text"]}

    element = {k.lstrip("@"): v for k, v in element.items()}

    # attributes and #text are unexpected if not a Workday ID element
    # attributes = [(k, v) for k, v in element.items() if k.startswith('@')]
    # if len(attributes) > 0:
    #     raise Exception(f"Unprocessed XML attributes found {attributes}")
    if "#text" in element:
        raise Exception(f"Text within element: {element}")

    return {key: transform_element(value) for key, value in element.items()}


def transform_id_elements(element):
    transformed_element = {}

    # this looks like gibberish but is the various ways ID elements are found
    # in a response from Workday and how ERP has chosen to transform them
    for item in element:
        # special parent id elements
        if (
            isinstance(item, dict)
            and "@parent_type" in item
            and "@parent_id" in item
        ):
            transformed_element[item["@parent_type"]] = item["@parent_id"]

        if isinstance(item, dict) and "@type" in item and "#text" in item:
            transformed_element[item["@type"]] = item["#text"]

        elif isinstance(item, dict) and "@System_ID" in item and "#text" in item:
            transformed_element[f'System_ID@{item["@System_ID"]}'] = item["#text"]

        # Unaccounted for shape of an ID element
        else:
            raise Exception(f"Unknown ID element found {item}")
    

    return transformed_element


maxInt = sys.maxsize

while True:
    # yucky but https://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
    # decrease the maxInt value by factor 2 as long as the OverflowError occurs

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt / 2)


def transform_xml(element, root_name):
    wrapped = "<Wrapped>" + element + "</Wrapped>"
    xmlDict = xmltodict.parse(wrapped)
    parsedField = transform_element(xmlDict)["Wrapped"]
    if parsedField is None:
        element = []
    else:
        element = next(iter(parsedField["root"].values()))
        if isinstance(element, dict):
            element = [element]

    return element


def transform_field(field, column_name, xml_columns):
    if isinstance(field, str) and column_name in xml_columns:
        return transform_xml(field, column_name)
    return field


transform_field_udf = udf(transform_field)

#Parsing Account_Set_Reference
try:
    xml_columns = ["Account_Set_Reference"]
    for col in df2.columns:
        if col.endswith('_Prompt'):
            df2 = df2.drop(col)
        else:
            df2 = df2.withColumn(col, transform_field_udf(F.col(col), F.lit(col), F.lit(xml_columns)))
except Exception as e:
    print(e)

# COMMAND ----------

try:
    if len(df2.collect())>0:
        #Forming the schema for the silver sub table customer_group_reference 

        subtableschema2 = ArrayType(
            StructType([
                StructField("WID", StringType(), True),
                StructField("Account_Set_ID", StringType(), True)
        ]))

        df2_new1=(df2.withColumn("Account_Set_Reference", F.from_json("Account_Set_Reference", subtableschema2))
            .selectExpr("inline(Account_Set_Reference)","load_datetime")
            )
        df2_new1=df2_new1.withColumnRenamed("load_datetime","__START_AT")
        df2_new1=df2_new1.withColumn("__END_AT",lit("null"))
        #Renaming the parsed columns
        df2_new1=df2_new1.withColumnRenamed("WID","ACCOUNT_SET_REFERENCE_WID")
        df2_new1=df2_new1.withColumnRenamed("Account_Set_ID","ACCOUNT_SET_ID")
        #Arrnaging in order
        silvercolorder2=[]
        silvercolorder2 = silvercolumns2.split(",")
        df2_new=df2_new1.select(silvercolorder2)
        #Overwriting the sub-silver table
        writetable_overwrite(df2_new,silver_full_table2)
except Exception as e:
    print(e)

# COMMAND ----------

#Control Table Insert with latest watermark value
max_date=spark.sql(f"Select max(load_datetime) from {bronze_table}").collect()[0][0]
spark.sql(f"Insert into {control_table}(bronze_table,load_datetime,updated_datetime) values ('{bronze_table_name}','{max_date}',from_unixtime(unix_timestamp(from_utc_timestamp(current_timestamp(), 'Australia/Melbourne'), 'dd.MM.yyyy HH:mm:ss.SSS')))")
