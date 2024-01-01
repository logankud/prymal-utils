'''
Prymal Utils - functions used for Prymal work on a recurring basis
'''


# -------------------------------------
# Requirements
# -------------------------------------
 
import pandas as pd
import os
import loguru
from loguru import logger
import boto3
from botocore.exceptions import ClientError



# -------------------------------------
# Functions
# -------------------------------------

# Check S3 Path for Existing Data
# -----------

def check_path_for_objects(bucket: str, s3_prefix:str):
  """
  Function to check an s3 path for any existing objects (typically used to make ETLs idempotent)

  Inputs:
    bucket: str, name of the bucket
    s3_prefix: str, the prefix of the objects to check for

  Outputs:
    objects_exist: bool, True if objects exist, False if not
  
  """

  logger.info(f'Checking for existing data in {bucket}/{s3_prefix}')

  # Instantiate objects_exist
  objects_exist=False
  
  try:

    # Create s3 client
    s3_client = boto3.client('s3', 
                            region_name = REGION,
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
  
    # List objects in s3_prefix
    result = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix )
  
    
  
    # Set objects_exist to true if objects are in prefix
    if 'Contents' in result:
        objects_exist=True
  
        logger.info('Data already exists!')

  # Botocore exception to handle client errors
  except ClientError as e:
    logger.error(e)

  return objects_exist

# Delete Existing Data from S3 Path
# -----------

def delete_s3_prefix_data(bucket:str, s3_prefix:str):
  """
  Function to delete all objects in an s3 prefix
  
  Inputs:
    bucket: str, name of the bucket
    s3_prefix: str, the prefix of the objects to delete
    
  Outputs:
    None
  """


  logger.info(f'Deleting existing data from {bucket}/{s3_prefix}')

  try:

    # Create an S3 client
    s3_client = boto3.client('s3', 
                            region_name = REGION,
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
  
    # Use list_objects_v2 to list all objects within the specified prefix
    objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)

  # Botocore exception to handle client errors
  except ClientError as e:
    logger.error(e)

  if 'Contents' in objects_to_delete:
  
    # Extract the list of object keys
    keys_to_delete = [obj['Key'] for obj in objects_to_delete.get('Contents', [])]
  
    # Check if there are objects to delete
    if keys_to_delete:
        # Delete the objects using 'delete_objects'
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={'Objects': [{'Key': key} for key in keys_to_delete]}
        )
        logger.info(f"Deleted {len(keys_to_delete)} objects")
    else:
        logger.info("No objects to delete")

  else:
    logger.info(f"No objects found in the specified prefix: {s3_prefix}"")