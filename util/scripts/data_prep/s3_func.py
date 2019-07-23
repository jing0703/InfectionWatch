# -*- coding: utf-8 -*-
"""
Functions to manage files in s3 bucket
"""

# REQUIRED MODULES
import os
from os.path import basename
import subprocess
import shutil
import logging
import boto3
from botocore.exceptions import ClientError

def upload_files(path):
    """Upload a file to an S3 bucket subfolder

    :param path: path for folder to upload
    """
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(S3_BUCKET)
 
    for subdir, dirs, files in os.walk(path):
        for f in files:  # TODO file is system variable
            try:
                full_path = os.path.join(subdir, f)
                with open(full_path, 'rb') as data:
                    bucket.put_object(Key=full_path[len(path)+1:], Body=data)
            except ClientError as e:  
                logging.error(e)

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


def del_obj(bucket, prefix):
    """delete files from s3 bucket"""
    bucket = s3_resource.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix=prefix):
        try:
            s3_resource.Object(bucket.name, obj.key).delete()
        except ClientError as e:
            logging.error(e)
