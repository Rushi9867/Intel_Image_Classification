import os,sys, pickle 
from src.intel.logger import logging 
from io import StringIO 
from src.intel.configuration.aws_connection import S3Client 
from mypy_boto3_s3.service_resource import Bucket 
from src.intel.exception import IntelException 
from botocore.exceptions import ClientEroor 
from pandas import DataFrame,read_csv 

class S3Operation:
    def __init__(self):
        s3_client = S3Client()
        self.s3_resource = s3_client.s3_resource
        self.s3_client = s3_client.s3_client

    def get_bucket(self,bucket_name: str) -> Bucket:

        logging.info("Entered the get_bucket method of S3Opeartions class")
        try: 
            bucket = self.s3_resource.Bucket(bucket_name)
            logging.info("Exited the get_bucket method of S3Operations class")
            return bucket 

        except Exception as e:
            raise IntelException(e,sys) from e 

    def download_file(self,bucket_name: str,output_file_path:str,key:str) -> None:
        logging.info("Entered the dowonload_file method of S3Operation class")
        try:
            self.s3_resource.Bucket(bucket_name).download_file(key,output_file_path) 
            logging.info("Exited the dowonload_file method of S3Operation class")
        
        except Exception as e:
            raise IntelException(e,sys) from e

    def read_data_from_s3(self,bucket_file_name: str,bucket_name: str,output_file_path:str) -> None:
        logging.info("Entered the read_data_from_s3 method of S3Operation class")
        try: 
            bucket = self.get_bucket(bucket_name)
            obj = bucket.download_file(key=bucket_filename,Filename=output_filepath)
            logging.info("exited the read_data_from_s3 method of S3Operation class")
            return obj 
        
        except Exception as e:
            raise IntelException(e,sys) from e