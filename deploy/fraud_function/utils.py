import pandas as pd

class DataLoader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = self.__load_data()

    def __load_data(self):
        try:
            data = pd.read_csv(self.file_path)
            return data
        except Exception as e:
            print(f"Error loading data: {e}")
            return None
    def get_data(self):
        return self.data

categorical_features = ['Payment Method', 'Product Category', 'Device Used']
numeric_features = ['Transaction Amount', 'Quantity', 'Customer Age', 'Account Age Days', 'Transaction Hour']
expanded_numeric_features = numeric_features + ['Amount_x_AccountAge', 'Quantity_per_AccountAge', 'Log_Transaction_Amount']

FEATURES_SCHEMA = {
    "Transaction Amount": ("transaction_amount", "DOUBLE"),
    "Quantity": ("quantity", "DOUBLE"),
    "Customer Age": ("customer_age", "DOUBLE"),
    "Account Age Days": ("account_age_days", "DOUBLE"),
    "Transaction Hour": ("transaction_hour", "DOUBLE"),
    "Transaction_DayNight": ("transaction_daynight", "INT64"),
    # Missing indicators
    "Transaction Amount_missing": ("transaction_amount_missing", "INT64"),
    "Quantity_missing": ("quantity_missing", "INT64"),
    "Customer Age_missing": ("customer_age_missing", "INT64"),
    "Account Age Days_missing": ("account_age_days_missing", "INT64"),
    "Transaction Hour_missing": ("transaction_hour_missing", "INT64"),
    "Payment Method_missing": ("payment_method_missing", "INT64"),
    "Product Category_missing": ("product_category_missing", "INT64"),
    "Device Used_missing": ("device_used_missing", "INT64"),
    # One-hot columns
    "Payment Method_bank transfer": ("payment_method_bank_transfer", "DOUBLE"),
    "Payment Method_credit card": ("payment_method_credit_card", "DOUBLE"),
    "Payment Method_debit card": ("payment_method_debit_card", "DOUBLE"),
    "Product Category_electronics": ("product_category_electronics", "DOUBLE"),
    "Product Category_health & beauty": ("product_category_health_beauty", "DOUBLE"),
    "Product Category_home & garden": ("product_category_home_garden", "DOUBLE"),
    "Product Category_toys & games": ("product_category_toys_games", "DOUBLE"),
    "Device Used_mobile": ("device_used_mobile", "DOUBLE"),
    "Device Used_tablet": ("device_used_tablet", "DOUBLE"),
    # Engineered
    "Amount_x_AccountAge": ("amount_x_accountage", "DOUBLE"),
    "Quantity_per_AccountAge": ("quantity_per_accountage", "DOUBLE"),
    "Log_Transaction_Amount": ("log_transaction_amount", "DOUBLE"),
}
