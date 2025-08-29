import sys, os
from dotenv import load_dotenv
from sagemaker.processing import ScriptProcessor, ProcessingInput, ProcessingOutput


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import categorical_features, numeric_features, expanded_numeric_features

load_dotenv("../../.env")
AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID")   
ROLE_ARN = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/sagemaker-execution-role"
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET")
MODEL_BUCKET = os.environ.get("MODEL_BUCKET")
DEPS_BUCKET = os.environ.get("DEPS_BUCKET")

script_processor = ScriptProcessor(
    image_uri=f"{AWS_ACCOUNT_ID}.dkr.ecr.us-east-1.amazonaws.com/fraud-pipeline", # TODO: update it with custom image
    command=["python3"],
    instance_type="ml.t3.medium",
    instance_count=1,
    role=ROLE_ARN,
)

inputs = [
    ProcessingInput(
        source=f"s3://{PROCESSED_BUCKET}/processed.csv",
        destination="/opt/ml/processing/input"
    ),
    ProcessingInput(
        source=f"s3://{DEPS_BUCKET}/libs/fraud_pipeline_deps.zip",
        destination="/opt/ml/processing/code"
    ),

]

outputs = [
    ProcessingOutput(
        source="/opt/ml/processing/output",
        destination=f"s3://{PROCESSED_BUCKET}/transformed"
    )
]


script_processor.run(
    code=f"s3://{DEPS_BUCKET}/feature_transformer_script.py",
    inputs=inputs,
    outputs=outputs,
    arguments=[
        "--categorical_features", ",".join(categorical_features),
        "--numeric_features", ",".join(numeric_features),
        "--expanded_numeric_features", ",".join(expanded_numeric_features)
    ]
)
