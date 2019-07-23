# -*- coding: utf-8 -*-
"""
Unzip bz2 format sequencing data from S3 bucket as raw sequencing data for analysis
"""

## REQUIRED MODULES
import sys
from os.path import dirname, abspath, join
root_dir = dirname(dirname(dirname(dirname(dirname(abspath(__file__))))))
sys.path.insert(0, root_dir)
from s3_func import upload_files
import boto3
import subprocess
import shutil
import logging
from botocore.exceptions import ClientError

def get_sample_name(sample):
    """get sample name from the file name"""
    return sample[:9]


def decompress_file():
    """download raw sequencing data from S3 bucket to untar file and upload unzip files to S3 bucket"""
    for sample in SAMPLE_LIST:
        try:
            s3.download_file(S3_BUCKET, sample, './{0}.tar.bz2'.format(get_sample_name(sample)))
            cmd = 'tar -jxf {0}'.format(sample)
            subprocess.check_output(cmd, shell=True)
            upload_files(get_sample_name(sample))
            os.unlink(get_sample_name(sample))
            shutil.rmtree(sample)
        except ClientError as e:
            logging.error(e)

if __name__ == '__main__':
    s3 = boto3.client('s3')  
    S3_BUCKET = 'sequencing-raw-data'
    SAMPLE_LIST =[key['Key'] for key in s3.list_objects(Bucket=S3_BUCKET)['Contents'] if key['Key'].endswith('.tar.bz2')]
    decompress_file()