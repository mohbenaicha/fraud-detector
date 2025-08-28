from model import FraudModel
from utils import DataLoader, categorical_features, numeric_features, expanded_numeric_features
from feature_transformer import FeatureTransformer
from artifact_manager import LocalArtifactManager as ArtifactManager
from data_processor import DataProcessor

def preprocess_and_transform(df, artifact_dir="artifacts"):
    processor = DataProcessor()
    df = processor.clean(df, numeric_features, categorical_features)
    df = processor.run_feature_engineering(df)
    ArtifactManager.BASE_DIR = artifact_dir
    artifact_manager = ArtifactManager()
    scaler = artifact_manager.load("fitted_scaler")
    ohe = artifact_manager.load("fitted_ohe")
    transformer = FeatureTransformer(scaler=scaler, ohe=ohe)
    df = transformer.transform(df, categorical_features, expanded_numeric_features)
    return df

def main():
    df = DataLoader("./Fraudulent_E-Commerce_Transaction_Data_2.csv").get_data()
    if df is None:
        print("Failed to load data.")
        return
    
    processor = DataProcessor()
    df = processor.clean(df, numeric_features, categorical_features)
    df = processor.run_feature_engineering(df)

    artifact_manager = ArtifactManager()

    scaler = artifact_manager.load("fitted_scaler")
    ohe = artifact_manager.load("fitted_ohe")

    transformer = FeatureTransformer(scaler=scaler, ohe=ohe)
    df = transformer.transform(df, categorical_features, expanded_numeric_features)
    model = FraudModel(artifact_manager.load("best_rf_model"))
    predictions = model.predict(df.drop('Is Fraudulent', axis=1).sample(100))
    print("Sample Predictions:", predictions)

if __name__ == "__main__":
    main()