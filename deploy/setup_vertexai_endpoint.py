import sys
import os
import warnings
from dotenv import load_dotenv
from google.cloud import aiplatform
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from artifact_manager import VertexAIModelManager
from inference import preprocess_and_transform

load_dotenv()
TEST = True
PROJECT_ID = os.environ.get("PROJECT_ID")
LOCATION = os.environ.get("LOCATION")
FEATURESTORE_ID = os.environ.get("FEATURESTORE_ID")
ENTITY_TYPE_ID = os.environ.get("ENTITY_TYPE_ID")
MODEL_DISPLAY_NAME = os.environ.get("MODEL_DISPLAY_NAME")
ENDPOINT_DISPLAY_NAME = os.environ.get("ENDPOINT_DISPLAY_NAME")
GCS_BUCKET = os.environ.get("GCS_BUCKET")

aiplatform.init(project=PROJECT_ID, location=LOCATION)

if not TEST:
    # Initialize Vertex AI
    print("Initializing Vertex AI...")

    model_name = "best_rf_model"
    manager = VertexAIModelManager(
        project_id=PROJECT_ID,
        location=LOCATION,
        model_group="fraud-prediction",
        gcs_bucket=GCS_BUCKET
    )
    manager.set_version("v1")
    model = manager.save(f"../artifacts/{model_name}.pkl", "model")

    endpoint = aiplatform.Endpoint.create(display_name=ENDPOINT_DISPLAY_NAME)
    print(f"Created endpoint: {endpoint.resource_name}")

    endpoint.deploy(
        model=model,
        deployed_model_display_name=MODEL_DISPLAY_NAME,
        traffic_percentage=100,
        machine_type="n1-standard-4",
    )
    print(f"Model deployed to endpoint: {endpoint.resource_name}")

featurestore_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/featurestores/{FEATURESTORE_ID}"
entity_type_name = f"{featurestore_name}/entityTypes/{ENTITY_TYPE_ID}"
entity_type = aiplatform.EntityType(entity_type_name)

def test_inference(test_entity_id: str):
    if TEST:
        print("retrieving endpoint resource...")
        endpoint = aiplatform.Endpoint(endpoint_name="8349852929540227072", project=PROJECT_ID, location=LOCATION)

    # reading from feature store
    features_df = entity_type.read(entity_ids=[test_entity_id])
    if features_df.empty:
        print(f"No features found for entity_id {test_entity_id}")
        return
    features = features_df.iloc[0].drop("entity_id").to_dict()

    df = pd.read_csv("../Fraudulent_E-Commerce_Transaction_Data_2.csv")
    df = df[df["Is Fraudulent"] == 1].sample(1)
    print("Target variable has value:", df["Is Fraudulent"].values[0])
    warnings.filterwarnings("ignore")
    df = preprocess_and_transform(df, "../artifacts").drop("Is Fraudulent", axis=1)
    features = df.iloc[0].to_dict()
        
    prediction = endpoint.predict(instances=[list(features.values())])
    print(f"Prediction for entity_id {test_entity_id}: {prediction.predictions[0]}")

# Example usage
test_entity_id = "test_entity_001" 
test_inference(test_entity_id)
