# -*- coding: utf-8 -*-
"""
Unzip sample sequencing data from S3 bucket as raw sequencing data
"""

## REQUIRED MODULES
import boto3
import os
import subprocess
import shutil
import logging
from botocore.exceptions import ClientError

def get_sample_name(sample):
    return sample[:9]

def upload_files(path):
    """Upload a file to an S3 bucket

    :param path: path for folder to upload
    """
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(S3_BUCKET)
 
    for subdir, dirs, files in os.walk(path):
        for file in files:
            try:
                full_path = os.path.join(subdir, file)
                with open(full_path, 'rb') as data:
                    bucket.put_object(Key=full_path[len(path)+1:], Body=data)
            except ClientError as e:
                logging.error(e)

#download raw sequencing data from S3 bucket to untar file and upload unzip files to S3 bucket
def decompress_file():
    for sample in SAMPLE_LIST:
        try:
            s3.download_file(S3_BUCKET, sample, './{0}.tar.bz2'.format(get_sample_name(sample)))
            cmd = 'tar -jxf {0}'.format(sample)
            subprocess.check_output(cmd, shell=True)
            upload_file(get_sample_name(sample))
            os.unlink(get_sample_name(sample))
            shutil.rmtree(sample)
        except ClientError as e:
            logging.error(e)

if __name__ == '__main__':
    s3 = boto3.client('s3')  
    S3_BUCKET = 'sequencing-raw-data'
    SAMPLE_LIST =[key['Key'] for key in s3.list_objects(Bucket=S3_BUCKET)['Contents'] if key['Key'].endswith('.tar.bz2')]
    decompress_file()


       
