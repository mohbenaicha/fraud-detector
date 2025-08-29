import boto3
import sys, os
from dotenv import load_dotenv
load_dotenv("../../.env")

glue = boto3.client("glue", region_name="us-east-1")  # change region
AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID")

response = glue.create_job(
    Name="FraudDataProcessorJob",
    Role=f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/glue-admin-role",  # TODO: update it with Glue Execution role
    ExecutionProperty={"MaxConcurrentRuns": 1},
    Command={
        "Name": "glueetl",
        "ScriptLocation": "s3://pipeline-deps/glue_data_processor.py",
        "PythonVersion": "3",
    },
    DefaultArguments={
        "--extra-py-files": "s3://pipeline-deps/libs/fraud_pipeline_deps.zip",
        "--additional-python-modules": "pandas",
    },
    MaxRetries=0,
    GlueVersion="3.0",
    WorkerType="Standard",
    NumberOfWorkers=2,
)

# Start the Glue job
glue.start_job_run(
    JobName="FraudDataProcessorJob",
    Arguments={
        "--INPUT_S3_PATH": "s3://fraud-raw/sample.csv",
        "--OUTPUT_S3_PATH": "s3://fraud-processed/processed.csv",
    },
)
