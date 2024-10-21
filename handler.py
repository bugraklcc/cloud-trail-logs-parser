from CloudTrail.src.Utils import S3Handler
from CloudTrail.src.DataParser.CloudTrailLogParser import CloudTrailLogParser
from datetime import datetime
import boto3
from icecream import ic

def get_latest_s3_key(bucket, prefix, date_str):
    """Find the latest CloudTrail file for a specific date in S3."""
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=f"{prefix}{date_str}/"
    )

    # Filter all files that end with .json.gz
    s3_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.json.gz')]

    if not s3_files:
        ic("No .json.gz file found for the specified date.")
        return None

    # Sort files by date and get the most recent one
    s3_files.sort()
    latest_file = s3_files[-1]
    return latest_file

def handler(event, context):
    S3_BUCKET = "your cloud trail bucket name"
    PREFIX = "your cloud trail bucket prefix"
    
    # Get the latest file for the relevant date (today's date or can be passed as a parameter)
    date_str = datetime.now().strftime('%Y/%m/%d')
    s3_key = get_latest_s3_key(S3_BUCKET, PREFIX, date_str)

    if not s3_key:
        ic("No file found to process.")
        return

    s3_handler = S3Handler()
    log_parser = CloudTrailLogParser()

    # Download the file from S3
    file_path = s3_handler.download_file(S3_BUCKET, s3_key)
    logs = s3_handler.extract_gzip_file(file_path)
    
    if logs is None:
        ic("Log file could not be downloaded or read.")
        return
    
    # Categorize logs
    indexed_logs = log_parser.categorize_logs(logs)

    logs_by_type = {}
    for record in indexed_logs:
        log_type = record.get('log_type', 'unknown')
        if log_type not in logs_by_type:
            logs_by_type[log_type] = []
        logs_by_type[log_type].append(record)

    for log_type, records in logs_by_type.items():
        # Transform logs to Parquet format
        parquet_data = log_parser.transform_to_parquet(records)
        if parquet_data:
            date_str_for_s3 = datetime.now().strftime('%Y-%m-%d')
            s3_key_output = f"data/generic_product_table/prod/daily/log_type={log_type}/data_date={date_str_for_s3}/{log_type}_log.parquet"
            s3_handler.upload_file(parquet_data, S3_BUCKET, s3_key_output)

if __name__ == "__main__":
    event = {}
    context = {}
    handler(event, context)
