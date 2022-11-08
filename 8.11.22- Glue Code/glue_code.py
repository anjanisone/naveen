import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def main(account_level):

    s3 = boto3.resource('s3')
    bucket_name = f"vgi-retail-{account_level}-us-east-1-rma-sandbox"
    bucket_path = "/ux2y/retail-acxiom-feed-test/"
    invalid_bucket_path = bucket_path+"invalid/"

    my_bucket = s3.Bucket(bucket_name)


    current_date = datetime.now.strftime("%Y%m%d")
    if current_date == "20220825":
        filename = f"RMIAL_retail_full_pii_{current_date}.txt"
        print("Current Date -", current_date)
        print("File Name -", filename)
    else:
        filename = f"RMIAL_retail_incr_pii_{current_date}.txt"


    csvs = []
    for object_summary in my_bucket.filter(prefix= bucket_path):
        if "/" not in object_summary.key.split(bucket_path)[1]:
            csvs.append(object_summary.key)
    filelist = csvs
    filecount = len(csvs)
    print(f"There are {filecount} CSV Files in the bucket {bucket_name}")
    print("The CSVS FIle List in the S3 is: ", filelist)
    


    ncsvs = []
    for object_summary in my_bucket.filter(prefix= invalid_bucket_path):
        if "/" not in object_summary.key.split(invalid_bucket_path)[1]:
            ncsvs.append(object_summary.key)
    filelist2 = ncsvs

    print("The CSVS FIle List in the S3 is: ", filelist2)


    #s3_client = boto3.client('s3')
    if filecount > 0:
        for file in filelist:
            print(f"Original FileName in s3 is: {file}")
            copy_source = {
                "Bucket": bucket_name,
                "Key": bucket_path
            }
            destination_bucket = bucket_name
            dest_key = bucket_path+filename
            try:
                response = s3.meta.client.copy(copy_source, destination_bucket, dest_key)
                s3.Object(bucket_name, file).delete()
                print(f"Move Successful of File from {file} to {bucket_path+filename}")
            except Exception as e:
                print("Move Failed because of :",e)
    else:
        for file in filelist2:
            print(f"Original FileName in s3 is: {file}")
            copy_source = {
                "Bucket": bucket_name,
                "Key": invalid_bucket_path
            }
            destination_bucket = bucket_name
            dest_key = invalid_bucket_path+filename
            try:
                response = s3.meta.client.copy(copy_source, destination_bucket, dest_key)
                s3.Object(bucket_name, file).delete()
                print(f"Move Successful of File from {file} to {invalid_bucket_path+filename}")
            except Exception as e:
                print("Move Failed because of :",e)


if __name__ == "__main__":
    sys_level = ""
    main(sys_level)