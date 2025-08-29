import sys, os
from dotenv import load_dotenv
from sagemaker.model import Model

load_dotenv("../../.env")
ROLE_ARN = os.environ.get("ROLE_ARN")
MODEL_S3_PATH = os.environ.get("MODEL_S3_PATH")
ENDPOINT_NAME = os.environ.get("ENDPOINT_NAME")
IMAGE_URI = os.environ.get("IMAGE_URI")

# Use generic Model for custom container
model = Model(
    image_uri=IMAGE_URI,
    model_data=MODEL_S3_PATH,
    role=ROLE_ARN
)

predictor = model.deploy(
    initial_instance_count=1,
    instance_type="ml.t3.medium",
    endpoint_name=ENDPOINT_NAME
)
