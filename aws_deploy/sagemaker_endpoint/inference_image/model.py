import multiprocessing
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.model_selection import ParameterSampler, cross_val_score
from sklearn.metrics import classification_report
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import KFold
from tqdm import tqdm

class FraudModel:
    def __init__(self, model=None):
        self.model = model

    def train(self, X, y):
        smote = SMOTE(random_state=42)
        X_resampled, y_resampled = smote.fit_resample(X, y)

        # 10. Train/test split
        X_train, self.X_test, y_train, self.y_test = train_test_split(
            X_resampled, y_resampled, test_size=0.2, random_state=42
        )

        # optionally: SMOTE, CV, hyperparameter tuning
        param_dist = {
            "n_estimators": np.arange(50, 151, 25),  # try 50–150 trees during search
            "max_depth": [None, 10, 20, 30, 40],
            "min_samples_split": np.arange(2, 11, 2),  # 2,4,6,8,10
            "min_samples_leaf": np.arange(1, 6),  # 1–5
            "max_features": ["sqrt", "log2", None],
            "bootstrap": [True, False],
        }
        param_list = list(ParameterSampler(param_dist, n_iter=60, random_state=42))

        best_score = -1
        best_params = None
        n_jobs = multiprocessing.cpu_count() - 1

        print(f"Searching {len(param_list)} parameter sets...")
        for params in tqdm(param_list):
            cv = KFold(n_splits=3, shuffle=True, random_state=42)
            model = RandomForestClassifier(**params, random_state=42, n_jobs=-1)
            scores = cross_val_score(model, X_train, y_train, cv=cv, scoring="f1_macro", n_jobs=n_jobs)
            mean_score = np.mean(scores)

            if mean_score > best_score:
                best_score = mean_score
                best_params = params

        # 12. Retrain best model on full training set with more estimators
        final_params = best_params.copy()
        final_params['n_estimators'] = 400   # override tuned value

        self.model = RandomForestClassifier(
            **final_params,
            random_state=42,
            n_jobs=n_jobs
        )

        self.model.fit(X_train, y_train)

    def evaluate(self):
        y_pred = self.model.predict(self.X_test)
        print(classification_report(self.y_test, y_pred))
        
    def predict(self, X):
        return self.model.predict(X)
