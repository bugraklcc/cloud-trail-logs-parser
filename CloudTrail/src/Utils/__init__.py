import boto3
import os
import gzip
import tempfile
import json

class S3Handler:
    def __init__(self):
        self.s3_client = boto3.client('s3')

    def download_file(self, bucket_name, key):
        """Downloads a file from S3 and saves it to a temporary folder."""
        local_file = os.path.join(tempfile.gettempdir(), "temp_file.json.gz")
        try:
            self.s3_client.download_file(bucket_name, key, local_file)
            return local_file
        except Exception as e:
            return None

    def extract_gzip_file(self, file_path):
        """Opens a GZ file and reads it as JSON."""
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                logs = json.load(f)
            return logs
        except Exception as e:
            return None

    def upload_file(self, output_data, bucket_name, s3_key):
        """Uploads data to S3."""
        try:
            self.s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=output_data)
        except Exception as e:
            print(f"S3 upload error: {e}")
