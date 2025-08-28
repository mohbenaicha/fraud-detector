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
