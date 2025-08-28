from utils import (
    DataLoader,
    categorical_features,
    numeric_features,
    expanded_numeric_features,
)
from data_processor import DataProcessor
from feature_transformer import FeatureTransformer
from artifact_manager import LocalArtifactManager as ArtifactManager
from model import FraudModel  # Assuming you have a model class defined


def main():
    # Load data
    df = DataLoader("./Fraudulent_E-Commerce_Transaction_Data_2.csv").get_data()

    if df is None:
        print("Failed to load data.")
        return

    # Process data
    processor = DataProcessor()
    df = processor.clean(df, numeric_features, categorical_features)

    # Feature engineering
    df = processor.run_feature_engineering(df)

    # Transform features
    transformer = FeatureTransformer()
    df = transformer.fit_transform(
        df, categorical_features, expanded_numeric_features
    )
    artifact_manager = ArtifactManager()
    artifact_manager.save(transformer.scaler, "fitted_scaler")
    artifact_manager.save(transformer.ohe, "fitted_ohe")
    model = FraudModel()
    model.train(df.drop("Is Fraudulent", axis=1), df["Is Fraudulent"])
    model.evaluate()
    artifact_manager.save(model.model, "best_rf_model")

if __name__ == "__main__":
    main()