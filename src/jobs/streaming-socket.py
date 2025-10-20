import json
import socket
import time
import pandas as pd

def handle_date(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)

def send_data_over_socket(file_path, host='spark-master', port=9999, chunk_size=2):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)
    print(f"Listening for connections on {host}:{port}")

    last_sent_index = 0
    df = pd.read_csv(file_path)

    # Convert object columns to string to avoid JSON issues
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str)

    while True:
        conn, addr = s.accept()
        print(f"Connection from {addr}")
        try:
            while last_sent_index < len(df):
                records = []
                for i in range(chunk_size):
                    if last_sent_index + i < len(df):
                        row = df.iloc[last_sent_index + i]
                        records.append(row.to_dict())
                    else:
                        break

                chunk_df = pd.DataFrame(records)
                print(chunk_df)

                for record in records:
                    serialized_data = json.dumps(record, default=handle_date).encode('utf-8')
                    conn.send(serialized_data + b'\n')
                    time.sleep(5)

                last_sent_index += chunk_size
        except (BrokenPipeError, ConnectionResetError):
            print("Client disconnected.")
        finally:
            conn.close()
            print("Connection closed")

if __name__ == "__main__":
    send_data_over_socket("/opt/bitnami/spark/datasets/Train_data1.csv")  
