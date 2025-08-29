import pandas as pd
import numpy as np
from typing import List

class DataProcessor:
    def __init__(self):
        pass

    def clean(self, df: pd.DataFrame, numeric_features: List[str], categorical_features: List[str]) -> pd.DataFrame:
        # drop irrelevant columns, handle missing values, rare categories
        # 1.Drop irrelevant columns
        df.drop(["Transaction ID","Customer ID","Customer Location","Transaction Date",
                "IP Address","Shipping Address","Billing Address"], axis=1, inplace=True)

        # 2. Handle missing in targte
        if 'Is Fraudulent' in df.columns:
            df = df.dropna(subset=['Is Fraudulent'])

        # 3.Handle missing numeric features
        for col in numeric_features:
            # Create a missing indicator
            df[col + "_missing"] = df[col].isna().astype(int)
            # Fill missing values with column median
            df[col].fillna({col: df[col].median()}, inplace=True)

        # 4. Handle missing categorical features
        for col in categorical_features:
            # Create a missing indicator
            df[col + "_missing"] = df[col].isna().astype(int)
            # Fill missing values with a placeholder
            df[col].fillna({col:"Unknown"}, inplace=True)
            # Replace rare categories with "Ohter'
            freq = df[col].value_counts(normalize=True)
            # identify rare categories (< 1% of data)
            rare_categories = freq[freq < 0.01].index
            # replace rare categories with 'other'
            df[col] = df[col].replace(rare_categories, "Other")
        return df

    def run_feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        # interaction terms, ratios, time bins, log transforms
        df['Amount_x_AccountAge'] = df['Transaction Amount'] * df['Account Age Days']
        df['Quantity_per_AccountAge'] = df['Quantity'] / (df['Account Age Days'] + 1e-6)
        df['Transaction_DayNight'] = np.where(df['Transaction Hour'].between(6, 18), 1, 0)
        df['Log_Transaction_Amount'] = np.log1p(df['Transaction Amount'])
        return df