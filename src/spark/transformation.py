# -*- coding: utf-8 -*-
"""
Batch processing by Spark to transform raw medical data and save to Redshift
"""

## REQUIRED MODULES
import os
import boto3
from datetime import datetime, date
from itertools import islice
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import col, to_timestamp,row_number
from pyspark.sql.types import *

# function to filter out header line and get conetnt from file
def get_content(data):
    return data.zipWithIndex().filter(lambda kv: kv[1] > 2).keys()

# function to calculate age from date of birth   
def calculate_age(born,admit_date):
    return admit_date.year - born.year - ((admit_date.month, admit_date.day) < (born.month, born.day))

#function for writing transformed dataframe to Redshift
def write_table(df,table):
    return df.write \
  .format("com.databricks.spark.redshift") \
  .option("url", "jdbc:redshift://CLUSGER/DB?user=USER&password=XXX") \
  .option("temporary_aws_access_key_id", "XXX") \
  .option("temporary_aws_secret_access_key", "XXX") \
  .option('forward_spark_s3_credentials',"true") \
  .option("dbtable", table) \
  .option("tempdir", "s3n://spark-temp-redshift1/tmp/") \
  .mode("append") \
  .save() 

# read raw data for pharmacy information as json and transform to dataframe
def transform_pharmacy_info():
    columns_to_drop = ['Day Of Service', 'Drug Class','Generic Drug','Route Of Administration','Route Of Administration Title','Therapeutic Category']
    ph_filtered = ph_content.drop(*columns_to_drop)
    ph_filtered.select(to_timestamp(ph['Date Of Service'], 'yyyy-MM-dd HH:mm:ss'))
    ph_df = ph_filtered.select(col("Discharge ID").alias("record_id"), col("Drug Class Title").alias("drug_class"), col("Therapeutic Category Title").alias("therapeutic_category"), col("Generic Drug Title").alias("generic_drug"),col("Adj Pharmacy Charges").alias("adj_pharmacy_charges"),col("Pharmacy Charges").alias("pharmacy_charges"),col("Date Of Service").alias("date_of_service"))
    write_table(ph_df,'pharmacy')

# read raw data for demographic information as csv and transform to dataframe
def transform_demographic_info():
    demographics_info = de_content.repartition(8).map(lambda p: Row(record_id=int(float(p[4])),
                                                                    hospital_city=p[1].replace('"',''),
                                                                    campus_name=p[3].replace('"','').replace('?',''),
                                                                    dob=datetime.strptime(p[10][1:-2], '%Y/%m/%d %H:%M:%S'),
                                                                    age=calculate_age(datetime.strptime(p[10][1:-2], '%Y/%m/%d %H:%M:%S'),datetime.strptime(p[25][1:-2], '%Y/%m/%d %H:%M:%S')),
                                                                    gender=p[12].replace('"',''),
                                                                    ethnicity=p[14].replace('"',''),
                                                                    patient_type=p[9].strip("''"),
                                                                    admit_date=datetime.strptime(p[25][1:-2], '%Y/%m/%d %H:%M:%S'),
                                                                    source_admission=p[30].replace('"',''),
                                                                    ICU_flage=p[32].replace('"',''),
                                                                    NICU_flage=p[33].replace('"',''),
                                                                    infection_flag=p[38].replace('"',''),
                                                                    total_days_in_ICU=float(p[73]),
                                                                    diagnosis_count=float(p[74]),
                                                                    renal_urologic_flag=p[70].replace('"',''),
                                                                    medical_complication_flage=p[39][1:-1],
                                                                    surgical_complication_flage=p[40][1:-1],
                                                                    ICDcode=p[50].strip("''")[1:4],
                                                                    ICDtitle=p[50].strip("''")[8:-1],
                                                                    severity_level=p[61].replace('"',''),
                                                                    risk_of_mortality=p[63].replace('"','')
                                                                    ))
                                                        de_df = sqlContext.createDataFrame(demographics_info)
    write_table(de_df,'demographics')

# read raw data for billing information as csv and transform to dataframe
def transform_billing_info():
    billing_info = de_content.repartition(8).map(lambda p: Row(record_id=int(float(p[4])                                        ),billing_no=p[5].strip("''"),
                                              billed_charges=float(p[75]),
                                              adj_billed_charges=float(p[76]),
                                              clinicalc_harges=float(p[77]),
                                              imaging_Charges=float(p[78]),
                                              lab_charges=float(p[79]),
                                              other_charges=float(p[80]),
                                              pharmacy_charges=float(p[81]),
                                              supply_charges=float(p[82]),
                                              adj_clinical_charges=float(p[83]),
                                              adj_imaging_charges=float(p[84]),
                                              adj_lab_charges=float(p[85]),
                                              adj_other_charges=float(p[86]),
                                              adj_pharmacy_charges=float(p[87]),
                                              adj_supply_charges=float(p[88])
                                             ))
    billing_df = sqlContext.createDataFrame(billing_info)
    write_table(billing_df,'billing')


if __name__ == '__main__':
    ph= SparkSession(sc).read.json("s3a://patient-metadata/pharmacy_info.csv")
    ph_content = get_content(ph)
    de = sc.textFile("s3a://patient-metadata/demographics_info.csv") \
    .map(lambda line: line.split(";")) \
    .filter(lambda line: len(line)>1)
    de_content = get_content(de)
    transform_pharmacy_info()
    transform_demographic_info()
    transform_billing_info()

