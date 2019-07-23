# -*- coding: utf-8 -*-
"""
Annotation for identified genes from sequence mapping reasult and write the analyzed infection result to redshift
"""

# REQUIRED MODULES
import boto3
import os
import csv  
import sys
import logging
from os.path import dirname, abspath, join
root_dir = dirname(dirname(dirname(abspath(__file__))))
sys.path.insert(0, root_dir)
import util.scripts.data_prep.s3_func
from Bio import Entrez
from Bio import SeqIO
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark.sql import SparkSession, SQLContext, Row
import pyspark.sql.functions as f
import pyspark.sql.types as t


def list_obj(split_char,bucket):
    '''get list of samples from s3 bucket for analysis'''
    return [key['Key'].split(split_char)[0] for key in s3.list_objects(Bucket=bucket)['Contents']]
    

def create_infection_df_schma():
    '''create dataframe to save analyzed infection result'''
    field = [StructField('index',IntegerType(), True),StructField('bacteria', StringType(), True),StructField('otu', IntegerType(), True)]
    schema = StructType(field)
    df = sqlContext.createDataFrame(sc.emptyRDD(), schema)
    return df


def get_patient_id(id_list):
    '''get patient's id from csv file and prepare to assagin the id for infection result'''
    patient_id = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("Auto configuration enabled","true").load(id_list)
    id_df = patient_id.rdd.flatMap(lambda p: Row(int(p[0]))).zipWithIndex().toDF(["record_id", "index"])
    return id_df


def get_taxonomy(gene_id):
    '''function to search for taxonomy id of genes from sequence mapping result'''
    taxonomy = []
    for key in gene_id:
        handle = Entrez.efetch(db='nuccore', id=key, rettype='gb')
        record = SeqIO.read(handle,'genbank')
        for k,v in record.annotations.items():
            if k == 'taxonomy':
                taxonomy.append(tuple((key, '_'.join(v)))) 
    return taxonomy


def data_transformation_by_rdd(sample, index):
    '''Using spark rdd to count the abundance of each infection type, transform result as infection type and abundance then union all infection types for each patient'''
    data = sc.textFile(sample).map(lambda x: x.split('\t'))
    clean_data = data.filter(lambda x: 'NC_' in x[8])
    gene = clean_data.map(lambda x: x[8])
    gene_count = gene.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
    gene_id = gene_count.map(lambda x: x[0]).collect()
    name_rdd = sc.parallelize(get_taxonomy(gene_id))
    annotation_rdd = name_rdd.join(gene_count).map(lambda x: (index-1,x[1][0],x[1][1])).toDF(["index", "bacteria","otu"])
    annotation_df = new_infection_df.union(annotation_rdd)
    infection_df = annotation_df.join(record_id,['index']).drop('index')
    return infection_df

def data_transformation_by_udf(sample):
    '''Using spark udf to count the abundance of each infection type, transform result as infection type and abundance then union all infection types for each patient'''
    data_df = spark.read.option('delimiter', '\t') \
                        .option('inferSchema', 'true') \
                        .csv(sample).toDF(['id','nt_length','accuracy','alignment_score','count','alignemnt_identity','strain','identification','gene','start','end'])
    clean_data_df = data_df.filter('NC_' in data_df['gene'])
    indexed_df = clean_data_df.groupby('gene').agg(f.count('gene').alias('otu')).rdd.zipWithIndex()
    index_df = sqlContext.createDataFrame(indexed_rdd)
    otu_df = index_df.select('gene', get_taxonomy('gene').alias('bacteria')).drop('gene')
    # otu_df = index_df.withColumn('bacteria',get_taxonomy(clean_data_df['gene'])).select('bacteria').drop('gene')
    annotation_df = new_infection_df.union(otu_df)
    infection_df = annotation_df.join(record_id,['index']).drop('index')
    return infection_df


def write_to_redshift(df):
    '''write the infection result for each patient to redshift'''
    if df.count()>1:
        df.write \
            .format("com.databricks.spark.redshift") \
            .option("url", "jdbc:redshift://YOUR_CLUSTER/YOUR_DB?user=USER_NAME&password=XXX") \
            .option("temporary_aws_access_key_id", os.environ['AWS_ACCESS_KEY']) \
            .option("temporary_aws_secret_access_key", os.environ['AWS_SECRET_ACCESS']) \
            .option('forward_spark_s3_credentials',"true") \
            .option("dbtable", "infection") \
            .option("tempdir", "s3n://YOUR_TEMP_BUSKET_FOR_REDSHIFT/") \
            .option("tempformat","CSV GZIP") \
            .mode("append") \
            .save()
    s3_func.del_obj(input_bucket, '{0}/'.format(mapping_result[n]))


def main():
    mapping_result = list_obj('/', input_bucket)
    for n in range(len(mapping_result)):
        if mapping_result[n] not in list_obj('.', output_bucket):
            s3_func.download_parts(mapping_result[n], input_bucket)
            s3.upload_file('{0}.txt'.format(mapping_result[n]), output_bucket, '{0}.txt'.format(mapping_result[n]))
            os.unlink('{0}.txt'.format(mapping_result[n])) 
            try:
                new_infection_df = create_infection_df_schma()
                f.register("get_taxonomy", get_taxonomy)
                data_transformation_by_udf("s3a://{0}/{1}.txt".format(output_bucket, mapping_result[n]), n)
                write_to_redshift(infection_df)
            except Exception as e:
                logging.fatal(e, exc_info=True)
        else:
            try:
                new_infection_df = create_infection_df_schma()
                data_transformation("s3a://{0}/{1}.txt".format(output_bucket, mapping_result[n]), n)
            except Exception as e:
                logging.fatal(e, exc_info=True)



if __name__ == '__main__':
    s3 = boto3.client('s3')  
    s3_resource = boto3.resource('s3')
    Entrez.email = "YOUR_EMAIL"
    conf = SparkConf().setAppName("PySpark App").setMaster("spark://ec2-107-21-13-191.compute-1.amazonaws.com:7077")
    SparkContext.setSystemProperty('spark.executor.memory', '6g')
    SparkContext.setSystemProperty('spark.driver.memory', '6g')
    sc = SparkContext(conf=conf).getOrCreate()
    sqlContext = SQLContext(sc)
    input_bucket = 'otu-result'
    output_bucket = 'finished-samples'
    record_id = get_patient_id("s3a://patient-metadata/patient_id.csv")
    main()