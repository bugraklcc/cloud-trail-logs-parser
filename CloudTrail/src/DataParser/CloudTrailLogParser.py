import pandas as pd
from io import BytesIO

class CloudTrailLogParser:
    def __init__(self):
        pass

    def categorize_logs(self, logs):
        """Categorizes the logs."""
        indexed_logs = []
        for index, record in enumerate(logs.get('Records', [])):
            event_source = record.get('eventSource')
            
            if event_source == 's3.amazonaws.com':
                indexed_logs.append(self._parse_s3_log(record))
            elif event_source == 'lambda.amazonaws.com':
                indexed_logs.append(self._parse_lambda_log(record))
            elif event_source == 'athena.amazonaws.com':
                indexed_logs.append(self._parse_athena_log(record))
        return indexed_logs

    def _parse_s3_log(self, record):
        parse_s3_result = {
            'user_who_accessed': record.get('userIdentity', {}).get('principalId', 'unknown'),
            'time_of_access': record.get('eventTime', None),
            'operation_type': record.get('eventName', None),
            'aws_region': record.get('awsRegion', None),
            'bucket_name': record.get('requestParameters', {}).get('bucketName', None),
            'key': record.get('requestParameters', {}).get('key', None),
            'source_ip_address': record.get('sourceIPAddress', None),
            'user_agent': record.get('userAgent', None),
            'log_type': 's3.amazonaws.com'
        }
        return parse_s3_result

    def _parse_lambda_log(self, record):
        parse_lambda_result = {
            'user_who_invoked': record.get('userIdentity', {}).get('principalId', 'unknown'),
            'lambda_resource_name': record.get('requestParameters', {}).get('functionName', None),
            'lambda_alias': record.get('requestParameters', {}).get('qualifier', None),
            'memory': record.get('requestParameters', {}).get('memorySize', None),
            'timeout': record.get('requestParameters', {}).get('timeout', None),
            'concurrency': record.get('requestParameters', {}).get('reservedConcurrentExecutions', None),
            'status': record.get('responseElements', {}).get('statusCode', None),
            'failure_description': record.get('errorMessage', None),
            'time_of_execution': record.get('eventTime', None),
            'aws_region': record.get('awsRegion', None),
            'source_ip_address': record.get('sourceIPAddress', None),
            'log_type': 'lambda.amazonaws.com'
        }
        return parse_lambda_result

    def _parse_athena_log(self, record):
        response_elements = record.get('responseElements', {})
        
        parse_athena_result = {
            'user_who_queried': record.get('userIdentity', {}).get('principalId', 'unknown'),
            'time_of_query': record.get('eventTime', None),
            'query_string': record.get('requestParameters', {}).get('queryString', None),
            'queried_database': record.get('requestParameters', {}).get('database', None),
            'query_status': response_elements.get('queryStatus', None) if response_elements else None,
            'result_location': response_elements.get('resultConfiguration', {}).get('outputLocation', None),
            'aws_region': record.get('awsRegion', None),
            'source_ip_address': record.get('sourceIPAddress', None),
            'user_agent': record.get('userAgent', None),
            'log_type': 'athena.amazonaws.com'
        }
        return parse_athena_result

        
    def transform_to_parquet(self, records):
        """Converts records into Parquet format."""
        if not records:
            return None

        df = pd.json_normalize(records)
        output_buffer = BytesIO()
        df.to_parquet(output_buffer, index=False)
        return output_buffer.getvalue()