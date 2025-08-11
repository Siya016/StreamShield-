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
    send_data_over_socket("/opt/bitnami/spark/datasets/Train_data1.csv")  # 👈 Change to your CSV path

#
# import json
# import socket
# import time
# import pandas as pd
#
#
# def handle_date(obj):
#     """
#     Custom JSON serializer for pandas Timestamp objects.
#     Converts them to a string format.
#     """
#     if isinstance(obj, pd.Timestamp):
#         return obj.strftime('%Y-%m-%d %H:%M:%S')
#     raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)
#
#
# def send_data_over_socket(file_path, host='0.0.0.0', port=9999, chunk_size=100):
#     """
#     Reads data from a CSV file, converts it to JSON, and sends it
#     over a TCP socket. This acts as the socket server.
#
#     Args:
#         file_path (str): The path to the CSV file.
#         host (str): The host IP address to bind to. Use '0.0.0.0' to listen on all interfaces.
#         port (int): The port number to listen on.
#         chunk_size (int): The number of rows to process and send in each chunk.
#                           Increased for better throughput with Spark.
#     """
#     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Crucial for immediate port reuse
#     s.bind((host, port))
#     s.listen(1)
#     print(f"Socket server listening for connections on {host}:{port}")
#
#     try:
#         df = pd.read_csv(file_path)
#         # Convert object columns to string to avoid JSON serialization issues
#         for col in df.select_dtypes(include=['object']).columns:
#             df[col] = df[col].astype(str)
#         print(f"Successfully loaded {len(df)} records from {file_path}")
#     except FileNotFoundError:
#         print(f"Error: CSV file not found at {file_path}")
#         s.close()
#         return
#     except Exception as e:
#         print(f"Error loading CSV: {e}")
#         s.close()
#         return
#
#     while True:  # Keep the server running to accept multiple connections if Spark restarts
#         conn, addr = s.accept()  # Accept a new connection
#         print(f"Accepted connection from {addr}")
#         try:
#             current_index = 0  # Always start from the beginning of the DataFrame for each new connection
#
#             while current_index < len(df):
#                 chunk_data_json_strings = []
#                 # Collect records for the current chunk as JSON strings
#                 for i in range(chunk_size):
#                     if current_index + i < len(df):
#                         row = df.iloc[current_index + i]
#                         chunk_data_json_strings.append(json.dumps(row.to_dict(), default=handle_date))
#                     else:
#                         break  # No more data to send
#
#                 if not chunk_data_json_strings:
#                     print("No more records to send for this connection. Waiting for next connection or loop.")
#                     break  # All data sent for this connection
#
#                 # Join all JSON strings in the chunk with newline and send as one large block
#                 # Ensure a trailing newline so Spark knows where the last record ends
#                 full_chunk_payload = "\n".join(chunk_data_json_strings) + "\n"
#
#                 try:
#                     conn.sendall(full_chunk_payload.encode('utf-8'))
#                     # print(f"Sent chunk of {len(chunk_data_json_strings)} records starting with: {chunk_data_json_strings[0][:50]}...") # Uncomment for verbose sending
#                 except (BrokenPipeError, ConnectionResetError) as e:
#                     print(f"Client disconnected during send: {e}. Accepting new connection.")
#                     break  # Break from inner loop to accept new connection
#
#                 current_index += len(chunk_data_json_strings)
#                 time.sleep(0.1)  # Small delay between sending chunks to avoid overwhelming Spark
#
#             print("Finished sending all data for current connection. Waiting for new client.")
#
#         except (BrokenPipeError, ConnectionResetError) as e:
#             print(f"Client disconnected or connection error: {e}. Accepting new connection.")
#         except Exception as e:
#             print(f"An unexpected error occurred during data sending: {e}")
#         finally:
#             conn.close()  # Ensure the connection is closed
#             print("Connection closed.")
#
#
# if __name__ == "__main__":
#     send_data_over_socket("/opt/bitnami/spark/datasets/Train_data1.csv")