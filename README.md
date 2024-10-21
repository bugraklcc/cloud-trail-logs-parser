CloudTrail Log Processing with AWS Lambda
This project processes AWS CloudTrail logs by downloading, categorizing, and transforming them into Parquet format. The processed logs are then uploaded to an S3 bucket for further use, such as querying with AWS Athena.

Table of Contents
Prerequisites
Project Structure
Installation
Usage
Deployment
License
Prerequisites
Before starting, ensure you have the following tools and setup:

AWS CLI: Install AWS CLI
AWS Account: You need access to an AWS account for Lambda and S3 services.
Python 3.8+: Make sure you have the appropriate Python version installed.
AWS IAM Permissions
Make sure the AWS IAM user/role you're using has the following permissions:

S3: Read and write access to your S3 bucket.
Lambda: Permissions to create, update, and invoke Lambda functions.
Project Structure
The project consists of several components that handle S3 interactions, log parsing, and transformation:

├── CloudTrail/
│   ├── src/
│   │   ├── Utils/
│   │   │   └── __init__.py   # Handles S3 operations (download, upload, gzip extraction)
│   │   └── DataParser/
│   │       └── CloudTrailLogParser.py  # Parses and categorizes CloudTrail logs
├── handler.py               # Main Lambda function handler
├── requirements.txt     # Python dependencies
└── README.md            # Project documentation (this file)

Installation
1. Clone the Repository
git clone https://github.com/your-repo/cloudtrail-lambda-processor.git
cd cloudtrail-lambda-processor
2. Install Python Dependencies
Create a virtual environment and install dependencies:

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
Update the requirements.txt file with the necessary dependencies. Here's an example:

boto3
pandas
pyarrow
3. Set Up AWS Credentials
Make sure your AWS credentials are properly configured. You can do this by running:

aws configure
This will prompt you to enter your AWS Access Key, Secret Key, region, and output format.

Usage
This Lambda function processes AWS CloudTrail logs by following these steps:

Download the latest .json.gz CloudTrail log file from the specified S3 bucket.
Extract and Parse the logs, categorizing them by event type (e.g., s3.amazonaws.com, lambda.amazonaws.com, athena.amazonaws.com).
Transform the logs into Parquet format using Pandas and PyArrow.
Upload the transformed Parquet file to an S3 bucket for querying in Athena.
Running Locally
If you want to test the code locally, run the following:

Make sure you're in the virtual environment:

source venv/bin/activate
Set environment variables required by the script:

export S3_BUCKET='your-s3-bucket-name'
export PREFIX='AWSLogs/'
Run the script:
python handler.py
Lambda Handler
The entry point for the Lambda function is app.lambda_handler. This handler function processes events, extracts logs, and pushes the transformed data to the S3 bucket.

def handler(event, context):
    # Add your Lambda function logic here
Deployment
1. Create Lambda Function
First, you need to package the Lambda function along with its dependencies. To do so:

Zip the source code and Python dependencies:

zip -r lambda_function.zip .
Make sure all dependencies are included in the zip file (e.g., boto3, pandas, pyarrow).

Create or Update the Lambda function using AWS CLI:

aws lambda create-function \
  --function-name CloudTrailLogProcessor \
  --runtime python3.8 \
  --role arn:aws:iam::YOUR_IAM_ROLE_ARN \
  --handler app.lambda_handler \
  --zip-file fileb://lambda_function.zip \
  --timeout 900 \
  --memory-size 1024
Alternatively, to update an existing Lambda function:

aws lambda update-function-code \
  --function-name CloudTrailLogProcessor \
  --zip-file fileb://lambda_function.zip
2. Set Environment Variables in Lambda
Go to the AWS Lambda Management Console and add the following environment variables to your Lambda function:

S3_BUCKET: The name of the S3 bucket where logs are stored.
PREFIX: The prefix for your CloudTrail logs.
3. Test the Lambda Function
Once deployed, you can test your Lambda function by manually invoking it via the AWS Management Console or by triggering it with an S3 event.

4. Automate the Process (Optional)
You can automate the triggering of the Lambda function using S3 Event Notifications or CloudWatch Events to process CloudTrail logs automatically as they arrive in the S3 bucket.

License
This project is licensed under the MIT License. See the LICENSE file for more details.