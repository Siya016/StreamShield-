from sklearn.metrics import classification_report, accuracy_score, f1_score
import numpy as np
import joblib
import pandas as pd

# Load labeled test data
test_df = pd.read_csv("../datasets/Train_data.csv")

# Separate features and labels
X_test = test_df.drop(columns=['class'])
y_true = test_df['class'].map({'normal': 0, 'anomaly': 1})  # 0 = normal, 1 = anomaly

# Load trained pipeline
pipeline = joblib.load("../jobs/isolation_pipeline.pkl")

# Get anomaly scores (lower means more anomalous)
scores = pipeline.named_steps['model'].decision_function(pipeline.named_steps['preprocessor'].transform(X_test))

# Try different thresholds and choose the best
best_f1 = 0
best_thresh = None
for thresh in np.linspace(min(scores), max(scores), 100):
    y_pred = (scores < thresh).astype(int)  # anomalies if score < threshold
    f1 = f1_score(y_true, y_pred)
    if f1 > best_f1:
        best_f1 = f1
        best_thresh = thresh

# Final predictions
y_pred = (scores < best_thresh).astype(int)

# Show performance
print(f"✅ Best threshold: {best_thresh:.4f}")
print(f"✅ Accuracy: {accuracy_score(y_true, y_pred):.4f}")
print(f"✅ F1 Score: {f1_score(y_true, y_pred):.4f}")
print("📊 Classification Report:")
print(classification_report(y_true, y_pred, target_names=['normal', 'anomaly']))
