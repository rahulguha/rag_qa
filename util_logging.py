import logging
import boto3
import io
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()

# from util import *
def get_now() ->str:
  now = datetime.now()
  formatted_date = now.strftime("%m-%d-%Y")
  return formatted_date
def get_rag_log_location() ->str:
    return os.getenv("RAG_LOG")
class S3LogHandler(logging.Handler):
   def __init__(self, bucket='podcast.monitor', prefix=get_rag_log_location()):
       super().__init__()
       self.s3_client = boto3.client('s3')
       self.bucket = bucket
       self.prefix = prefix + f"{get_now()}"
       self.log_buffer = io.StringIO()

   def emit(self, record):
       try:
           # Format log message
           msg = self.format(record)
           # Append to existing log file or create new one
           log_key = f"{self.prefix}/current_rag_log.log"
        #    print(get_now())
           
        #    log_key = f"{self.prefix}{formatted_date}.log"
           try:
               # Try to get existing log content
               existing_log = self.s3_client.get_object(Bucket=self.bucket, Key=log_key)
               existing_content = existing_log['Body'].read().decode('utf-8')
               print (existing_content)
           except self.s3_client.exceptions.NoSuchKey:
               existing_content = ''

           # Append new log message
           updated_content = existing_content + msg + '\n'
           
           # Upload updated content
           self.s3_client.put_object(
               Bucket=self.bucket, 
               Key=log_key, 
               Body=updated_content.encode('utf-8')
           )
       except Exception as e:
           print(f"S3 Log Upload Failed: {e}")


def setup_logger(bucket='podcast.monitor', prefix=get_rag_log_location()):
#    console_handler = logging.StreamHandler(sys.stdout)

    logger = logging.getLogger('S3Logger')
    logging_level= os.getenv("LOGGINGLEVEL")
   
    if logging_level == "DEBUG":
        logger.setLevel(logging.DEBUG)
    elif logging_level == "INFO":
        logger.setLevel(logging.INFO)
    elif logging_level == "ERROR":
        logger.setLevel(logging.ERROR)
    else:
        logger.setLevel(logging.DEBUG)
    
    # print (logger.level)
    # S3 Handler
    s3_handler = S3LogHandler(bucket, prefix)
    s3_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(s3_handler)

    # Console Handler
    #    console_handler = logging.StreamHandler()
    #    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    #    logger.addHandler(console_handler)

    return logger