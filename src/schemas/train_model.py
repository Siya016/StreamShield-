# train_model.py
import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib

df = pd.read_csv('../datasets/Train_data1.csv')

# Select numeric fields only
numeric_features = df.select_dtypes(include=["int", "float"]).fillna(0)

model = IsolationForest(contamination=0.01, random_state=42)
model.fit(numeric_features)

joblib.dump(model, "../jobs/isolation_forest.pkl")
