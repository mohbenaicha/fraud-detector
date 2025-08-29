import base64
import json
import os
from dotenv import load_dotenv
import pandas as pd
from google.cloud import bigquery
from google.cloud import aiplatform
from feature_transformer import FeatureTransformer
from artifact_manager import LocalArtifactManager as ArtifactManager
from data_processor import DataProcessor
from utils import categorical_features, numeric_features, expanded_numeric_features, FEATURES_SCHEMA

# Vertex AI & Feature Store config
load_dotenv() # currently .env doesn't contain sensitive testing so it is packaged for cloud function
PROJECT_ID = os.environ.get("PROJECT_ID")
LOCATION = os.environ.get("LOCATION")
FEATURESTORE_ID = os.environ.get("FEATURESTORE_ID")
ENTITY_TYPE_ID = os.environ.get("ENTITY_TYPE_ID")
ENDPOINT_ID = os.environ.get("ENDPOINT_ID")

# Initialize Vertex AI
aiplatform.init(project=PROJECT_ID, location=LOCATION)
entity_type = aiplatform.EntityType(
    f"projects/{PROJECT_ID}/locations/{LOCATION}/featurestores/{FEATURESTORE_ID}/entityTypes/{ENTITY_TYPE_ID}"
)
endpoint = aiplatform.Endpoint(f"projects/{PROJECT_ID}/locations/{LOCATION}/endpoints/{ENDPOINT_ID}")

# Initialize BigQuery client
bq_client = bigquery.Client(project=PROJECT_ID)
DATASET_ID = "fraud_transactions"
TABLE_ID = "transactions"
BQ_TABLE = f"{DATASET_ID}.{TABLE_ID}"

def main(event, context):
    """Cloud Function triggered by Pub/Sub message"""
    try:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(pubsub_message)
        df = pd.DataFrame([data])

        processor = DataProcessor()
        df = processor.clean(df, numeric_features, categorical_features)
        df = processor.run_feature_engineering(df)

        ArtifactManager.BASE_DIR = None
        artifact_manager = ArtifactManager()
        scaler = artifact_manager.load("fitted_scaler")
        ohe = artifact_manager.load("fitted_ohe")
        transformer = FeatureTransformer(scaler=scaler, ohe=ohe)
        df = transformer.transform(df, categorical_features, expanded_numeric_features)
        
        df.to_gbq(BQ_TABLE, project_id=PROJECT_ID, if_exists='append')
        # Rename and type-define DataFrame columns to match feature store schema names and types
        entity_id = data.get("transaction_id", "temp_entity")

        feature_values = {}
        for col in df.columns:
            if col in FEATURES_SCHEMA:
                schema_name, schema_type = FEATURES_SCHEMA[col]
                value = df[col].iloc[0]  # get first row value
                # convert to Python native type
                if schema_type == "DOUBLE":
                    value = float(value)
                elif schema_type == "INT64":
                    value = int(value)
                feature_values[schema_name] = value
                print(f"Column {col} renamed to {schema_name} with type {schema_type} and value {value}")

        entity_type.write_feature_values({entity_id: feature_values})

        features_df = entity_type.read(entity_ids=[entity_id])
        features = features_df.iloc[0].drop("entity_id").to_dict()
        prediction = endpoint.predict(instances=[list(features.values())])

        print(f"Prediction for entity_id {entity_id}: {prediction}")

        return prediction

    except Exception as e:
        print(f"Error in processing: {e}")
        raise e


