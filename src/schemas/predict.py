import joblib
import pandas as pd

# ✅ Load the saved IsolationForest pipeline
pipeline = joblib.load("../jobs/isolation_pipeline.pkl")

# ✅ Sample test input (replace with dynamic input if needed)
sample_data = {
  "duration": 0,
  "protocol_type": "tcp",
  "service": "private",
  "flag": "REJ",
  "src_bytes": 0,
  "dst_bytes": 0,
  "land": 0,
  "wrong_fragment": 0,
  "urgent": 0,
  "hot": 0,
  "num_failed_logins": 0,
  "logged_in": 0,
  "num_compromised": 0,
  "root_shell": 0,
  "su_attempted": 0,
  "num_root": 0,
  "num_file_creations": 0,
  "num_shells": 0,
  "num_access_files": 0,
  "num_outbound_cmds": 0,
  "is_host_login": 0,
  "is_guest_login": 0,
  "count": 205,
  "srv_count": 12,
  "serror_rate": 0.0,
  "srv_serror_rate": 0.0,
  "rerror_rate": 1.0,
  "srv_rerror_rate": 1.0,
  "same_srv_rate": 0.06,
  "diff_srv_rate": 0.06,
  "srv_diff_host_rate": 0.0,
  "dst_host_count": 255,
  "dst_host_srv_count": 12,
  "dst_host_same_srv_rate": 0.05,
  "dst_host_diff_srv_rate": 0.07,
  "dst_host_same_src_port_rate": 0.0,
  "dst_host_srv_diff_host_rate": 0.0,
  "dst_host_serror_rate": 0.0,
  "dst_host_srv_serror_rate": 0.0,
  "dst_host_rerror_rate": 1.0,
  "dst_host_srv_rerror_rate": 1.0
}
# ✅ Convert input to DataFrame
input_df = pd.DataFrame([sample_data])

# ✅ Make prediction
prediction = pipeline.predict(input_df)

# ✅ Interpret result
if prediction[0] == -1:
    print("🚨 Anomaly detected!")
else:
    print("✅ Normal connection.")

