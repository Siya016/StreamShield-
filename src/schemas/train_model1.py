import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import joblib

# Load dataset
df = pd.read_csv('../datasets/Train_data1.csv')

# Identify categorical and numerical columns
categorical_cols = ['protocol_type', 'service', 'flag']
numerical_cols = [col for col in df.columns if col not in categorical_cols]

# Preprocessing
preprocessor = ColumnTransformer(transformers=[
    ('num', StandardScaler(), numerical_cols),
    ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_cols)
])

# Create pipeline
pipeline = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('model', IsolationForest(n_estimators=200, contamination=0.5, random_state=42, verbose=1))
])

# Fit the model
pipeline.fit(df)

# Save model
joblib.dump(pipeline, "../jobs/isolation_pipeline1.pkl")
print("✅ IsolationForest pipeline model saved!")
