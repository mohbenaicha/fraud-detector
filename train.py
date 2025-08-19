import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import ParameterSampler, cross_val_score
from sklearn.metrics import classification_report
from imblearn.over_sampling import SMOTE
from tqdm import tqdm



df = pd.read_csv("./Fraudulent_E-Commerce_Transaction_Data_2.csv")

df.drop(["Transaction ID","Customer ID","Customer Location","Transaction Date","IP Address","Shipping Address","Billing Address"], axis=1, inplace=True)

df['Payment Method'] = pd.Categorical(df['Payment Method'], 
                                      categories=["debit card","credit card","PayPal","bank transfer"]).codes

df['Product Category'] = pd.Categorical(df['Product Category'],
                                        categories=["home & garden","electronics","toys & games","clothing","health & beauty"]).codes

df['Device Used'] = pd.Categorical(df['Device Used'],
                                    categories=["desktop","mobile","tablet"]).codes

new_column_order = ['Transaction Amount', 'Payment Method', 'Product Category', 'Quantity',
                   'Customer Age', 'Device Used','Address Match' , 'Account Age Days',
                   'Transaction Hour','Is Fraudulent']
df = df.reindex(columns=new_column_order)

numeric_features = ['Transaction Amount', 'Quantity', 'Customer Age', 'Account Age Days', 'Transaction Hour']
scaler = StandardScaler()
df[numeric_features] = scaler.fit_transform(df[numeric_features])

X = df.drop('Is Fraudulent', axis=1)
y = df['Is Fraudulent']




# SMOTE oversampling
smote = SMOTE(random_state=42)
X_resampled, y_resampled = smote.fit_resample(X, y)

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(
    X_resampled, y_resampled, test_size=0.2, random_state=42
)

# --- Expanded hyperparameter space ---
param_dist = {
    'n_estimators': np.arange(50, 151, 25),   # try 50–150 trees during search
    'max_depth': [None, 10, 20, 30, 40],
    'min_samples_split': np.arange(2, 11, 2), # 2,4,6,8,10
    'min_samples_leaf': np.arange(1, 6),      # 1–5
    'max_features': ['sqrt', 'log2', None],
    'bootstrap': [True, False]
}

# sample ~60 parameter sets (expand if you have more time)
param_list = list(ParameterSampler(param_dist, n_iter=60, random_state=42))

best_score = -1
best_params = None

print(f"Searching {len(param_list)} parameter sets...")
for params in tqdm(param_list):
    model = RandomForestClassifier(**params, random_state=42, n_jobs=-1)
    scores = cross_val_score(model, X_train, y_train, cv=3, scoring="f1_macro", n_jobs=-1)
    mean_score = np.mean(scores)

    if mean_score > best_score:
        best_score = mean_score
        best_params = params

print("\nBest params from tuning:", best_params, "with CV f1_macro:", best_score)

# --- Retrain best model on full training set with more trees ---
final_params = best_params.copy()
final_params['n_estimators'] = 400   # override tuned value

best_rf = RandomForestClassifier(
    **final_params,
    random_state=42,
    n_jobs=-1
)

X_train,X_test,y_train,y_test = train_test_split(X_resampled,y_resampled,test_size=0.2,random_state=42)


best_rf.fit(X_train, y_train)
y_pred = best_rf.predict(X_test)

print("\nFinal Evaluation on Test Set:")
print(classification_report(y_test, y_pred))

import joblib
joblib.dump(best_rf, 'best_rf_model.pkl')