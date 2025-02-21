# import torch, whisper
import json
import os
import re

from dotenv import load_dotenv
load_dotenv()
import glob

# import yt_dlp
import sys
from datetime import datetime, timedelta
from datetime import date, datetime
from s3Connect import S3Uploader

from util_logging import S3LogHandler
from util_logging import *

# logger = setup_logger()
# logger.info("Starting data processing")
# logger = setup_logger()
logger = setup_logger()

def log(level, message):
   level_map = {
       'debug': logger.debug,
       'info': logger.info,
       'warning': logger.warning,
       'error': logger.error,
       'critical': logger.critical
   }
   
   log_method = level_map.get(level.lower())
   if log_method:
       log_method(message)
   else:
       logger.warning(f"Invalid log level: {level}. Message: {message}")
def filter_dates_after_cutoff(dates):
    cutoff_date = get_cutoff_date()
    cutoff = datetime.strptime(cutoff_date, '%m-%d-%Y')
    return [date for date in dates if datetime.strptime(date, '%m-%d-%Y') > cutoff]


def get_cutoff_date():
  cutoff_date = os.getenv("CUTOFFDATE")
  year, month, day = cutoff_date.split('/')
    
    # Rearrange the components
  return f"{month}-{day}-{year}"
  # now = datetime.now()
  # formatted_date = now.strftime("%m-%d-%Y")
  # return formatted_date
   
def generate_python_friendly_filename(input_string):
  """
  Generates a Python-friendly filename from a given string.

  Args:
    input_string: The input string to be converted.

  Returns:
    A Python-friendly filename string.
  """

  # 1. Remove invalid characters
  filename = re.sub(r'[\\/:"*?<>|]', '', input_string)  # Remove invalid characters

  # 2. Replace spaces and other whitespace with underscores
  filename = filename.replace(" ", "_") 

  # 3. Convert to lowercase (optional)
  filename = filename.lower()

  # 4. Limit filename length (optional)
  max_length = 255  # Adjust as needed
  filename = filename[:max_length] 

  return filename
def strip_extension(filename):
    """
    Strips the extension from a filename.
    
    Args:
    filename (str): The filename including the extension.
    
    Returns:
    str: The filename without the extension.
    """
    return os.path.splitext(filename)[0]
def load_json(filename):
    with open(filename, 'r') as f:
        data = json.load(f)
        return data
def check_file(file_name):
    if not os.path.exists(file_name):
        return False
    else: 
        return True
def strip_before_last_slash(string):
    return string.rsplit('/', 1)[-1]
def create_file(filename, content):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:   # Opens file and casts as f 
        f.write(content )       # Writing
def get_now() ->str:
  now = datetime.now()
  formatted_date = now.strftime("%m-%d-%Y")
  return formatted_date

def date_gap_dates_list(date_str):
    """Returns a list of dates (strings) between today and a past date (inclusive),
       formatted as MM-DD-YYYY.

    Args:
        past_date_str: A string representing the past date in YYYY-MM-DD format.

    Returns:
        A list of strings representing the dates in the gap (inclusive of past and today),
        formatted as MM-DD-YYYY.
        Returns None if the date string is invalid.
    """
     # Convert the input date string to a datetime object
    input_date = datetime.strptime(date_str, "%m-%d-%Y")
    
    # Get the current date
    current_date = datetime.now()
    
    # Calculate the gap in days
    gap = (current_date - input_date).days
    
    # Generate an array of date strings in mm-dd-yyyy format
    date_array = [(input_date + timedelta(days=i)).strftime("%m-%d-%Y") for i in range(gap + 1)]
    
    return date_array
def my_hook(d):
    if d['status'] == 'downloading':
        print ("###### downloading "+ str(round(float(d['downloaded_bytes'])/float(d['total_bytes'])*100,1))+"%")
    if d['status'] == 'finished':
        filename=d['###### filename']
        print(filename)

def str_to_datetime(date_string, input_type="%a, %d %b %Y %H:%M:%S %z" ):
  """
  Converts a string in the format "Mon, 30 Dec 2024 02:04:44 -0000" 
  to a Python datetime object.

  Args:
    date_string: The string representing the date and time.

  Returns:
    A datetime object representing the date and time, 
    or None if the string cannot be parsed.
  TODO:
    Add another arg to take input of date type to expect - regular expression
  """
  try:
    # return datetime.strptime(date_string, "%a, %d %b %Y %H:%M:%S %z")
    return datetime.strptime(date_string, input_type)
  except ValueError:
    print(f"Invalid date/time format: {date_string}")
    return None
def check_folder_exists(folder_path):
  """
  Checks if a folder exists at the given path.

  Args:
    folder_path: The path to the folder.

  Returns:
    True if the folder exists, False otherwise.
  """
  return os.path.isdir(folder_path)

def stop_program_if_condition(condition):
  """
  Stops program execution if the given condition is True.

  Args:
    condition: The boolean condition to evaluate.
  """
  if condition:
    sys.exit(0)  # Exit the program with a success code (0)
  
# S3 related 
def download_s3_bucket_flat( local_dir, prefix="", aws_region="us-east-1"):
    """
    Downloads all files from an S3 bucket, flattens the structure, and overwrites duplicates with the latest version.

    :param bucket_name: Name of the S3 bucket.
    :param local_dir: Local directory to save files.
    :param prefix: Folder (prefix) inside the bucket to download (e.g., "transcriptions/").
    :param aws_region: AWS region (optional).
    """
    s3 = boto3.client("s3", region_name=aws_region) if aws_region else boto3.client("s3")
    bucket_name = get_bucket_name()
    # Ensure the local directory exists
    os.makedirs(local_dir, exist_ok=True)
    print (f"{get_local_rag_path()}/**/*.txt")
    existing_files = glob.glob(f"{get_local_rag_path()}/**/*.txt", recursive=True)  # Get all .txt files in subdirectories
    
    for i in range(len(existing_files)):
      existing_files[i] = strip_before_last_slash(existing_files[i])

    # print(existing_files)
    # Dictionary to track latest file versions
    latest_files = {}

    # List all objects under the specified prefix
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                filename = os.path.basename(key)  # Extract just the filename (flatten structure)
                if filename not in existing_files:
                  last_modified = obj["LastModified"]

                  # If file exists, only keep the latest version
                  if filename not in latest_files or latest_files[filename]["LastModified"] < last_modified:
                      latest_files[filename] = {"Key": key, "LastModified": last_modified}

    # Download the latest versions of files
    for filename, file_info in latest_files.items():
        s3_key = file_info["Key"]
        local_file_path = os.path.join(local_dir, filename)

        print(f"Downloading {s3_key} as {local_file_path} (latest version)...")
        s3.download_file(bucket_name, s3_key, local_file_path)

    print("Download complete.")

def get_bucket_name() ->str:
   return os.getenv("S3_BUCKET")
def get_local_rag_path() -> str:
   return os.getenv("RAG_LOCAL_DATA_PATH")
def get_rag_embedding() -> str:
   return os.getenv("RAG_EMBEDDING")
def read_s3_file(  key) ->str:
  formatted_date = get_now()  

  uploader = S3Uploader(
      bucket_name=get_bucket_name(),
      prefix=""  # All files will be uploaded under this prefix
    )
  return uploader.read_s3_file(key)
def get_s3_folders(type):
  folders = []
  for f in S3Uploader.list_s3_folders1(f"{get_bucket_name()}", f"{type}"):
     folders.append (f.split('/')[-2])    
  return (filter_dates_after_cutoff(folders))



