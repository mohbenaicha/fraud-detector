import os, sys
from dotenv import load_dotenv
import pandas as pd
from sagemaker.predictor import Predictor
from sagemaker.serializers import CSVSerializer
from sagemaker.deserializers import JSONDeserializer

load_dotenv("../../.env")

ENDPOINT_NAME = os.environ.get("ENDPOINT_NAME")
INPUT_CSV = "../sample.csv"  # already preprocessed features

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from inference import preprocess_and_transform

predictor = Predictor(
    endpoint_name=ENDPOINT_NAME,
    serializer=CSVSerializer(),    # send CSV rows
    deserializer=JSONDeserializer()  # receive JSON predictions
)

df = pd.read_csv(INPUT_CSV)
df = preprocess_and_transform(df)
if "Is Fraudulent" in df.columns:
    input_data = df.drop("Is Fraudulent", axis=1)
else:
    input_data = df

predictions = predictor.predict(input_data)
print("Sample predictions:", predictions[:10])
