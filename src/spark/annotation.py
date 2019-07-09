# -*- coding: utf-8 -*-
"""
Download sample sequencing data from Human Microbiome Project website from a list of sample links
"""

## REQUIRED MODULES
import boto3
import os
import csv  
import sys
import logging
from botocore.exceptions import ClientError
from Bio import Entrez
from Bio import SeqIO
from os.path import basename
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import *


def download_parts(base_object, bucket_name, output_name=None, limit_parts=0):
    """Download all mapping result into a single file"""
    base_object = base_object.rstrip('/')
    bucket = boto3.resource('s3').Bucket(bucket_name)
    prefix = '{}/part-'.format(base_object)
    write_name = basename(base_object)+'.txt'
    output_name = output_name  or write_name

    with open(output_name, 'ab') as outfile:
        for i, obj in enumerate(bucket.objects.filter(Prefix=prefix)):
            bucket.download_fileobj(obj.key, outfile)
            if limit_parts and i >= limit_parts:
                print('Terminating download after {} parts.'.format(i))
                break
            else:
                print('Download completed after {} parts.'.format(i))

def list_obj(split_char,bucket):
    return [key['Key'].split(split_char)[0] for key in s3.list_objects(Bucket=bucket)['Contents']]
    # [key['Key'].split('/')[0] for key in s3.list_objects(Bucket='otu-result')['Contents'] if key['Size']>0]
    # [key['Key'].split('.')[0] for key in s3.list_objects(Bucket='finished-samples')['Contents'] if key['Key'].endswith('.txt')]

def del_obj(bucket,prefix):
    bucket = s3_resource.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix=prefix):
        try:
            s3_resource.Object(bucket.name, obj.key).delete()
        except ClientError as e:
            logging.error(e)

def create_infection_df_schma():
    field = [StructField('index',IntegerType(), True),StructField('bacteria', StringType(), True),StructField('otu', IntegerType(), True)]
    schema = StructType(field)
    df = sqlContext.createDataFrame(sc.emptyRDD(), schema)
    return df

def get_patient_id(id_list):
    patient_id = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("Auto configuration enabled","true").load(id_list)
    id_df = patient_id.rdd.flatMap(lambda p: Row(int(p[0]))).zipWithIndex().toDF(["record_id", "index"])
    return id_df

def get_taxonomy(gene_id):
    taxonomy = []
    for key in gene_id:
        handle = Entrez.efetch(db='nuccore', id=key, rettype='gb')
        record = SeqIO.read(handle,'genbank')
        for k,v in record.annotations.items():
            if k == 'taxonomy':
                taxonomy.append(tuple((key, '_'.join(v)))) 
    return taxonomy

def data_transformation(sample,index):
    
    data=sc.textFile(sample).map(lambda x: x.split('\t'))
    clean_data = data.filter(lambda x: 'NC_' in x[8])
    gene = clean_data.map(lambda x: x[8])
    gene_count = gene.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
    gene_id = gene_count.map(lambda x: x[0]).collect()
    name_rdd = sc.parallelize(get_taxonomy(gene_id))
    annotation_rdd = name_rdd.join(gene_count).map(lambda x: (index-1,x[1][0],x[1][1])).toDF(["index", "bacteria","otu"])
    annotation_df = new_infection_df.union(annotation_rdd)
    infection_df = annotation_df.join(record_id,['index']).drop('index')
    infection_df.write \
                .format("com.databricks.spark.redshift") \
                .option("url", "jdbc:redshift://CLUSGER/DB?user=USER&password=XXX") \
                .option("temporary_aws_access_key_id", "XXX") \
                .option("temporary_aws_secret_access_key", "XXX") \
                .option('forward_spark_s3_credentials',"true") \
                .option("dbtable", "infection") \
                .option("tempdir", "s3n://spark-temp-redshift1/tmp/") \
                .option("tempformat","CSV GZIP") \
                .mode("append") \
                .save()
    del_obj(input_bucket,'{0}/'.format(mapping_result[n]))

def main():
    mapping_result = list_obj('/',input_bucket)
    for n in range(len(mapping_result)):
        if mapping_result[n] not in list_obj('.',output_bucket):
            download_parts(mapping_result[n],input_bucket)
            s3.upload_file('{0}.txt'.format(mapping_result[n]), output_bucket, '{0}.txt'.format(mapping_result[n]))
            os.unlink('{0}.txt'.format(mapping_result[n])) 
            try:
                new_infection_df = create_infection_df_schma()
                data_transformation("s3a://{0}/{1}.txt".format(output_bucket,mapping_result[n]),n)
            except Exception as e:
                logging.fatal(e, exc_info=True)
                print('check',mapping_result[n])
        else:
            try:
                new_infection_df = create_infection_df_schma()
                data_transformation("s3a://{0}/{1}.txt".format(output_bucket,mapping_result[n]),n)
            except Exception as e:
                logging.fatal(e, exc_info=True)
                print('check',mapping_result[n])



if __name__ == '__main__':
    s3 = boto3.client('s3')  
    s3_resource = boto3.resource('s3')
    Entrez.email = "vera.jxu@gmail.com"
    # conf = SparkConf().setAppName("PySpark App").setMaster("spark://ec2-3-215-106-221.compute-1.amazonaws.com:7077").set("spark.speculation","false")
    conf = SparkConf().setAppName("PySpark App").setMaster("spark://ec2-107-21-13-191.compute-1.amazonaws.com:7077")
    SparkContext.setSystemProperty('spark.executor.memory', '6g')
    SparkContext.setSystemProperty('spark.driver.memory', '6g')
    sc = SparkContext(conf=conf).getOrCreate()
    sqlContext = SQLContext(sc)
    input_bucket = 'otu-result'
    output_bucket = 'finished-samples'
    record_id = get_patient_id("s3a://patient-metadata/testingID_1.csv")
    main()

    
  
