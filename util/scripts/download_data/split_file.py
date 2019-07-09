# -*- coding: utf-8 -*-
"""
Divide raw sequencing data larger than 10G to small files
"""

## REQUIRED MODULES
from Bio import SeqIO
import boto3
import os
import csv  
import logging
from botocore.exceptions import ClientError

def get_folder_name(sample):
    return sample[:9]

def get_sample_name(sample):
    return sample[10:]

def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    copy_source = {
        'Bucket': bucket_from_name,
        'Key': file_name
    }
    s3.Object(bucket_to_name, file_name).copy(copy_source)

def batch_iterator(iterator, batch_size):
    """Returns lists of length batch_size.

    This can be used on any iterator, for example to batch up
    SeqRecord objects from Bio.SeqIO.parse(...), or to batch
    Alignment objects from Bio.AlignIO.parse(...), or simply
    lines from a file handle.

    This is a generator function, and it returns lists of the
    entries from the supplied iterator.  Each list will have
    batch_size entries, although the final list may be shorter.
    """
    entry = True  # Make sure just loop once
    while entry:
        batch = []
        while len(batch) < batch_size:
            try:
                entry = next(iterator)
            except StopIteration:
                entry = None
            if entry is None:
                # End of file
                break
            batch.append(entry)
        if batch:
            yield batch

def divide_file():
    for sample,size in SAMPLE_INFO.items():
        if get_folder_name(sample) not in UPLOAD_LIST and size>1000000000:
            try:
                my_bucket.download_file(sample,'./{0}'.format(get_sample_name(sample)))
                record_iter = SeqIO.parse(open('./{0}'.format(get_sample_name(sample))),"fastq")        
                for i, batch in enumerate(batch_iterator(record_iter, 10000000)):
                    filename = get_folder_name(sample) + '_'+ sample[-7] + "_%i.fastq" % (i + 1)
                    with open(filename, "w") as handle:
                        count = SeqIO.write(batch, handle, "fastq")
                        conn.upload_file(filename, 'split-seq', filename)
                        os.system('rm ./{0}'.format(filename))
                    print("Wrote %i records to %s" % (count, filename))
                os.unlink(get_sample_name(sample))
            except:
                logging.error(e)


if __name__ == '__main__':
    s3 = boto3.client('s3')  
    DOWNLOAD_BUCKET = 'sequencing-raw-data'
    UPLOAD_BUCKET = 'split-seq'
    DOWNLOAD_LIST =[key['Key'] for key in conn.list_objects(Bucket=DOWNLOAD_BUCKET)['Contents'] if key['Key'].endswith('.fastq')]
    SAMPLE_SIZE = [key['Size'] for key in conn.list_objects(Bucket=DOWNLOAD_BUCKET)['Contents'] if key['Key'].endswith('.fastq')]
    SAMPLE_INFO = dict(zip(sample_list, sample_size))
    UPLOAD_LIST =[key['Key'][:9] for key in s3.list_objects(Bucket=UPLOAD_BUCKET)['Contents']]