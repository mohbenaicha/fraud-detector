import os
import pickle
from model import FraudModel

def model_fn(model_dir):
    """Load model from SageMaker container"""
    with open(os.path.join(model_dir, "best_rf_model.pkl"), "rb") as f:
        return FraudModel(pickle.load(f))

def predict_fn(input_data, model):
    """Run inference on input DataFrame"""
    return model.predict(input_data)