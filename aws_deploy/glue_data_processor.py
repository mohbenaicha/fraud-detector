# updloaded to AWS GLUE for ETL processing

import sys
import boto3
from awsglue.utils import getResolvedOptions
from data_processor import DataProcessor
from utils import numeric_features, categorical_features
from utils import DataLoader  # assuming it can load from S3

# Glue passes script arguments via sys.argv
args = getResolvedOptions(sys.argv, ["INPUT_S3_PATH", "OUTPUT_S3_PATH"])
input_s3 = args["INPUT_S3_PATH"]
output_s3 = args["OUTPUT_S3_PATH"]

# Load CSV from S3
df = DataLoader(input_s3).get_data()

# Process
processor = DataProcessor()
df = processor.clean(df, numeric_features, categorical_features)
df = processor.run_feature_engineering(df)

# Write processed CSV back to S3
df.to_csv(output_s3, index=False)
print(f"Processed data written to {output_s3}")
