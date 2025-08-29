import sys
import os
import zipfile
import pandas as pd
# Install additional packages if requirements.txt is present 
# requirements_path = "/opt/ml/processing/code/requirements.txt"
# if os.path.exists(requirements_path):
#     subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", requirements_path])

with zipfile.ZipFile("/opt/ml/processing/code/fraud_pipeline_deps.zip", "r") as zip_ref:
    zip_ref.extractall("/opt/ml/processing/code")

for root, dirs, files in os.walk("/opt/ml/processing"):
    print(root, dirs, files)
    
# Add your .zip dependencies to sys.path
sys.path.append("/opt/ml/processing/code")

from s3_artifact_manager import S3ArtifactManager
from feature_transformer import FeatureTransformer
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--categorical_features", type=str, required=True)
    parser.add_argument("--numeric_features", type=str, required=True)
    parser.add_argument("--expanded_numeric_features", type=str, required=True)
    return parser.parse_args()

def main():
    args = parse_args()
    categorical_features = args.categorical_features.split(",")
    expanded_numeric_features = args.expanded_numeric_features.split(",")

    input_dir = "/opt/ml/processing/input"
    output_dir = "/opt/ml/processing/output"

    # Assuming only one CSV in input_dir
    input_files = [f for f in os.listdir(input_dir) if f.endswith(".csv")]
    if not input_files:
        raise ValueError("No CSV files found in input directory")
    df = pd.read_csv(os.path.join(input_dir, input_files[0]))

    # Load artifacts
    artifact_manager = S3ArtifactManager(bucket_name="fraud-model-store")
    transformer = FeatureTransformer(
        scaler=artifact_manager.load("fitted_scaler.pkl"),
        ohe=artifact_manager.load("fitted_ohe.pkl")
    )

    df_transformed = transformer.transform(df, categorical_features, expanded_numeric_features)

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "transformed.csv")
    df_transformed.to_csv(output_path, index=False)
    print(f"Transformed data written to {output_path}")

if __name__ == "__main__":
    main()
