import pandas as pd
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler

class FeatureTransformer:
    def __init__(self, scaler: StandardScaler = None, ohe: OneHotEncoder = None):
        self.scaler = scaler or StandardScaler()
        self.ohe = ohe or OneHotEncoder(drop='first', sparse_output=False, handle_unknown='ignore')

    def fit_transform(self, df: pd.DataFrame, categorical_features: list, numeric_features: list) -> pd.DataFrame:
        # Fit & transform categorical features
        cat_array = self.ohe.fit_transform(df[categorical_features])
        df_cat = pd.DataFrame(cat_array, columns=self.ohe.get_feature_names_out(categorical_features), index=df.index)
        df = pd.concat([df.drop(columns=categorical_features), df_cat], axis=1)
        # Scale numeric
        df[numeric_features] = self.scaler.fit_transform(df[numeric_features])
        return df

    def transform(self, df: pd.DataFrame, categorical_features: list, numeric_features: list) -> pd.DataFrame:
        cat_array = self.ohe.transform(df[categorical_features])
        df_cat = pd.DataFrame(cat_array, columns=self.ohe.get_feature_names_out(categorical_features), index=df.index)
        df = pd.concat([df.drop(columns=categorical_features), df_cat], axis=1)
        df[numeric_features] = self.scaler.transform(df[numeric_features])
        return df