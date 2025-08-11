# from pyspark.sql import  SparkSession
# from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
# from pyspark.sql.functions import from_json, col, when, udf

#
# def start_streaming(spark):
#     try:
#         stream_df = (spark.readStream.format("socket")
#                      .option("host", "localhost")
#                      .option("port", 9999)
#                      .load()
#                      )
#         query = stream_df.writeStream.outputMode("append").format("console").start()
#         query.awaitTermination()
#         schema = StructType([
#             StructField("duration", IntegerType(), True),
#             StructField("protocol_type", StringType(), True),
#             StructField("service", StringType(), True),
#             StructField("flag", StringType(), True),
#             StructField("src_bytes", IntegerType(), True),
#             StructField("dst_bytes", IntegerType(), True),
#             StructField("land", IntegerType(), True),
#             StructField("wrong_fragment", IntegerType(), True),
#             StructField("urgent", IntegerType(), True),
#             StructField("hot", IntegerType(), True),
#             StructField("num_failed_logins", IntegerType(), True),
#             StructField("logged_in", IntegerType(), True),
#             StructField("num_compromised", IntegerType(), True),
#             StructField("root_shell", IntegerType(), True),
#             StructField("su_attempted", IntegerType(), True),
#             StructField("num_root", IntegerType(), True),
#             StructField("num_file_creations", IntegerType(), True),
#             StructField("num_shells", IntegerType(), True),
#             StructField("num_access_files", IntegerType(), True),
#             StructField("num_outbound_cmds", IntegerType(), True),
#             StructField("is_host_login", IntegerType(), True),
#             StructField("is_guest_login", IntegerType(), True),
#             StructField("count", IntegerType(), True),
#             StructField("srv_count", IntegerType(), True),
#             StructField("serror_rate", FloatType(), True),
#             StructField("srv_serror_rate", FloatType(), True),
#             StructField("rerror_rate", FloatType(), True),
#             StructField("srv_rerror_rate", FloatType(), True),
#             StructField("same_srv_rate", FloatType(), True),
#             StructField("diff_srv_rate", FloatType(), True),
#             StructField("srv_diff_host_rate", FloatType(), True),
#             StructField("dst_host_count", IntegerType(), True),
#             StructField("dst_host_srv_count", IntegerType(), True),
#             StructField("dst_host_same_srv_rate", FloatType(), True),
#             StructField("dst_host_diff_srv_rate", FloatType(), True),
#             StructField("dst_host_same_src_port_rate", FloatType(), True),
#             StructField("dst_host_srv_diff_host_rate", FloatType(), True),
#             StructField("dst_host_serror_rate", FloatType(), True),
#             StructField("dst_host_srv_serror_rate", FloatType(), True),
#             StructField("dst_host_rerror_rate", FloatType(), True),
#             StructField("dst_host_srv_rerror_rate", FloatType(), True)
#         ])
#         stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select("data.*")
#         query = stream_df.writeStream.outputMode("append").format('console'.options(truncate=False).start())
#         query.awaitTermination()
#
#
#     except Exception as e:
#         print(e)
#
#
# if __name__ == "__main__":
#     spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()
#
#     start_streaming(spark_conn)


# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
# from pyspark.sql.functions import from_json, col
# from src.config.config import config
#
#
#
# def start_streaming(spark):
#     topic = 'network_topic'
#     while True:
#         try:
#             # Define schema before using it
#             schema = StructType([
#                 StructField("duration", IntegerType(), True),
#                 StructField("protocol_type", StringType(), True),
#                 StructField("service", StringType(), True),
#                 StructField("flag", StringType(), True),
#                 StructField("src_bytes", IntegerType(), True),
#                 StructField("dst_bytes", IntegerType(), True),
#                 StructField("land", IntegerType(), True),
#                 StructField("wrong_fragment", IntegerType(), True),
#                 StructField("urgent", IntegerType(), True),
#                 StructField("hot", IntegerType(), True),
#                 StructField("num_failed_logins", IntegerType(), True),
#                 StructField("logged_in", IntegerType(), True),
#                 StructField("num_compromised", IntegerType(), True),
#                 StructField("root_shell", IntegerType(), True),
#                 StructField("su_attempted", IntegerType(), True),
#                 StructField("num_root", IntegerType(), True),
#                 StructField("num_file_creations", IntegerType(), True),
#                 StructField("num_shells", IntegerType(), True),
#                 StructField("num_access_files", IntegerType(), True),
#                 StructField("num_outbound_cmds", IntegerType(), True),
#                 StructField("is_host_login", IntegerType(), True),
#                 StructField("is_guest_login", IntegerType(), True),
#                 StructField("count", IntegerType(), True),
#                 StructField("srv_count", IntegerType(), True),
#                 StructField("serror_rate", FloatType(), True),
#                 StructField("srv_serror_rate", FloatType(), True),
#                 StructField("rerror_rate", FloatType(), True),
#                 StructField("srv_rerror_rate", FloatType(), True),
#                 StructField("same_srv_rate", FloatType(), True),
#                 StructField("diff_srv_rate", FloatType(), True),
#                 StructField("srv_diff_host_rate", FloatType(), True),
#                 StructField("dst_host_count", IntegerType(), True),
#                 StructField("dst_host_srv_count", IntegerType(), True),
#                 StructField("dst_host_same_srv_rate", FloatType(), True),
#                 StructField("dst_host_diff_srv_rate", FloatType(), True),
#                 StructField("dst_host_same_src_port_rate", FloatType(), True),
#                 StructField("dst_host_srv_diff_host_rate", FloatType(), True),
#                 StructField("dst_host_serror_rate", FloatType(), True),
#                 StructField("dst_host_srv_serror_rate", FloatType(), True),
#                 StructField("dst_host_rerror_rate", FloatType(), True),
#                 StructField("dst_host_srv_rerror_rate", FloatType(), True)
#             ])
#
#             # Read raw streaming data from socket
#             stream_df = (spark.readStream
#                          .format("socket")
#                          .option("host", "0.0.0.0")
#                          .option("port", 9999)
#                          .load())
#
#             # Parse JSON from 'value' column into structured fields
#             parsed_df = stream_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
#
#             # parsed_df.printSchema()
#             # parsed_df.show(5)
#
#             kafka_df = parsed_df.selectExpr("CAST(duration AS STRING) AS key", "to_json(struct(*)) AS value")
#
#             query = (kafka_df.writeStream
#                      .format("kafka")
#                      .option("kafka.bootstrap.servers", config["kafka"]["bootstrap.servers"])
#                      .option("topic", topic)
#                      .option("kafka.security.protocol", config["kafka"]["security.protocol"])
#                      .option("kafka.sasl.mechanism", config["kafka"]["sasl.mechanisms"])
#                      .option("kafka.ssl.ca.location", config["kafka"]["ssl.ca.location"])
#                      .option("kafka.sasl.jaas.config",
#                              f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{config["kafka"]["sasl.username"]}" password="{config["kafka"]["sasl.password"]}";')
#                      .option("checkpointLocation", "/tmp/spark-kafka-checkpoint")
#                      .option("kafka.client.id", "spark-stream")
#
#                      .start())
#             query.awaitTermination()
#
#             # query = (kafka_df.writeStream
#             #          .format("kafka")
#             #          .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
#             #          .option("kafka.security.protocol", config['kafka']['security.protocol'])
#             #          .option('kafka.sasl.mechanism', config['kafka']['sasl.mechanisms'])
#             #          .option('kafka.sasl.jaas.config',
#             #                  'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
#             #                  'password="{password}";'.format(
#             #                      username=config['kafka']['sasl.username'],
#             #                      password=config['kafka']['sasl.password']
#             #                  ))
#             #          .option('checkpointLocation', '/tmp/checkpoint')
#             #          .option('topic', topic)
#             #          .start())
#             # query.awaitTermination()
#
#
#
#
#         except Exception as e:
#             print("Error:", e)
#
#
# if __name__ == "__main__":
#     spark_conn = SparkSession.builder\
#         .appName("SocketStreamConsumer") \
#         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
#         .getOrCreate()
#     start_streaming(spark_conn)


#
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
# from pyspark.sql.functions import from_json, col, to_json, struct
#
# config = {
#     "kafka": {
#         "bootstrap.servers": "kafka-21ad1bb3-ghrietn-d652.h.aivencloud.com:21239",
#         "sasl.mechanisms": "PLAIN",
#         "sasl.username": "avnadmin",
#         "sasl.password": "AVNS_xs5OKHRFUD5s8CeUISS",
#         "security.protocol": "SASL_SSL",
#         # --- NEW: TrustStore configuration ---
#         # This points to the JKS file created from your ca.pem using keytool
#         "ssl.truststore.location": "/opt/bitnami/spark/config/kafka.client.truststore.jks",
#         # Path to the JKS file INSIDE Docker
#         "ssl.truststore.password": "changeit",
#         "ssl.ca.location": "/opt/bitnami/spark/config/ca.pem"
#
#     }
# }
#
#
# def start_streaming(spark):
#     """
#     Starts the Spark Structured Streaming job to read from a socket,
#     parse JSON, and write to an Aiven Kafka topic.
#     """
#     topic = 'network_topic'
#
#     # Define schema for the incoming JSON data
#     schema = StructType([
#         StructField("duration", IntegerType(), True),
#         StructField("protocol_type", StringType(), True),
#         StructField("service", StringType(), True),
#         StructField("flag", StringType(), True),
#         StructField("src_bytes", IntegerType(), True),
#         StructField("dst_bytes", IntegerType(), True),
#         StructField("land", IntegerType(), True),
#         StructField("wrong_fragment", IntegerType(), True),
#         StructField("urgent", IntegerType(), True),
#         StructField("hot", IntegerType(), True),
#         StructField("num_failed_logins", IntegerType(), True),
#         StructField("logged_in", IntegerType(), True),
#         StructField("num_compromised", IntegerType(), True),
#         StructField("root_shell", IntegerType(), True),
#         StructField("su_attempted", IntegerType(), True),
#         StructField("num_root", IntegerType(), True),
#         StructField("num_file_creations", IntegerType(), True),
#         StructField("num_shells", IntegerType(), True),
#         StructField("num_access_files", IntegerType(), True),
#         StructField("num_outbound_cmds", IntegerType(), True),
#         StructField("is_host_login", IntegerType(), True),
#         StructField("is_guest_login", IntegerType(), True),
#         StructField("count", IntegerType(), True),
#         StructField("srv_count", IntegerType(), True),
#         StructField("serror_rate", FloatType(), True),
#         StructField("srv_serror_rate", FloatType(), True),
#         StructField("rerror_rate", FloatType(), True),
#         StructField("srv_rerror_rate", FloatType(), True),
#         StructField("same_srv_rate", FloatType(), True),
#         StructField("diff_srv_rate", FloatType(), True),
#         StructField("srv_diff_host_rate", FloatType(), True),
#         StructField("dst_host_count", IntegerType(), True),
#         StructField("dst_host_srv_count", IntegerType(), True),
#         StructField("dst_host_same_srv_rate", FloatType(), True),
#         StructField("dst_host_diff_srv_rate", FloatType(), True),
#         StructField("dst_host_same_src_port_rate", FloatType(), True),
#         StructField("dst_host_srv_diff_host_rate", FloatType(), True),
#         StructField("dst_host_serror_rate", FloatType(), True),
#         StructField("dst_host_srv_serror_rate", FloatType(), True),
#         StructField("dst_host_rerror_rate", FloatType(), True),
#         StructField("dst_host_srv_rerror_rate", FloatType(), True)
#     ])
#
#     try:
#         # Read raw streaming data from socket.
#         # Spark is the CLIENT, connecting to the 'spark-master' service
#         # (where your Python sender script is running as a server).
#         stream_df = (spark.readStream
#                      .format("socket")
#                      .option("host", "spark-master")  # Connect to the service name of the sender
#                      .option("port", 9999)
#                      .load())
#
#         # Parse JSON from 'value' column into structured fields
#         parsed_df = stream_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
#
#         # --- Debugging Step: Print parsed data to console ---
#         # Uncomment this block to verify if Spark is receiving and parsing data correctly.
#         # If data appears here, the socket connection is good, and the issue is with Kafka.
#         print("Starting console sink for debugging...")
#         query_console = parsed_df.writeStream \
#             .outputMode("append") \
#             .format("console") \
#             .trigger(processingTime="5 seconds") \
#             .option("truncate", "false")\
#             .start()
#
#         # query_console.start() # This .start() should be correctly indented
#         # query_console.awaitTermination(timeout=60) # Run for 60 seconds to observe output, then proceed
#         # -------------------------------------------------
#
#         # Prepare data for Kafka: key as string, value as JSON string of the whole row
#         kafka_df = parsed_df.selectExpr("CAST(duration AS STRING) AS key", "to_json(struct(*)) AS value")
#
#         # --- Debugging Step 2: Print the config dictionary being used ---
#         print("\n--- Kafka Configuration being used ---")
#         print(config)
#         print("--------------------------------------\n")
#
#         print("Attempting to write to Kafka...")
#
#         query = (kafka_df.writeStream
#                  .format("kafka")
#                  .option("kafka.bootstrap.servers", config["kafka"]["bootstrap.servers"])
#                  .option("topic", topic)
#                  .option("kafka.security.protocol", config["kafka"]["security.protocol"])
#                  .option("kafka.sasl.mechanism", config["kafka"]["sasl.mechanisms"])
#                  # # Use the new TrustStore options
#                  # .option("kafka.ssl.truststore.location", config["kafka"]["ssl.truststore.location"])
#                  # .option("kafka.ssl.truststore.password", config["kafka"]["ssl.truststore.password"])
#                  .option("kafka.ssl.ca.location", config["kafka"]["ssl.ca.location"])
#                  .option("kafka.sasl.jaas.config",
#                          f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{config["kafka"]["sasl.username"]}" password="{config["kafka"]["sasl.password"]}";')
#                  .option("checkpointLocation", "/tmp/spark-kafka-checkpoint") # Ensure this path is writable
#                  # .option("kafka.client.id", "spark-stream")
#                  .outputMode("append") \
#                  .start()) # This .start() should be correctly indented
#
#         query_console.awaitTermination()
#         query.awaitTermination()  # Block until the query terminates
#
#     except Exception as e:
#         print(f"An error occurred in Spark streaming: {e}")
#         # In a production scenario, you might want more robust error handling,
#         # logging, or graceful shutdown.
#
#
# if __name__ == "__main__":
#     # Initialize Spark Session
#     spark_conn = SparkSession.builder \
#         .appName("SocketStreamConsumer") \
#         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
#         .getOrCreate()
#
#     # Set log level to INFO for more detailed output (optional, but helpful for debugging)
#     # spark_conn.sparkContext.setLogLevel("INFO")
#
#     start_streaming(spark_conn)




# from pyspark.sql import SparkSession
# from pyspark.sql.pandas.functions import pandas_udf
# from pyspark.sql.types import *
# from pyspark.sql.functions import from_json, col, to_json, struct
# import pickle
# import pandas as pd
#
# # Local Kafka running in Docker
# config = {
#     "kafka": {
#         "bootstrap.servers": "kafka:9092"  # Use 'kafka' if spark is running in Docker Compose with Kafka
#     }
# }
#
# # Load pre-trained Isolation Forest model
# with open("/opt/bitnami/spark/jobs/isolation_forest.pkl", "rb") as f:
#     isolation_model = pickle.load(f)
#
#
# # Define Pandas UDF for anomaly detection
# @pandas_udf("integer")
# def predict_anomaly_udf(*cols):
#     df = pd.concat(cols, axis=1)
#     preds = isolation_model.predict(df)  # -1: anomaly, 1: normal
#     return pd.Series([1 if x == -1 else 0 for x in preds])  # 1 = anomaly, 0 = normal
#
#
# def start_streaming(spark):
#     topic = 'network_topic'
#
#     schema = StructType([
#         StructField("duration", IntegerType(), True),
#         StructField("protocol_type", StringType(), True),
#         StructField("service", StringType(), True),
#         StructField("flag", StringType(), True),
#         StructField("src_bytes", IntegerType(), True),
#         StructField("dst_bytes", IntegerType(), True),
#         StructField("land", IntegerType(), True),
#         StructField("wrong_fragment", IntegerType(), True),
#         StructField("urgent", IntegerType(), True),
#         StructField("hot", IntegerType(), True),
#         StructField("num_failed_logins", IntegerType(), True),
#         StructField("logged_in", IntegerType(), True),
#         StructField("num_compromised", IntegerType(), True),
#         StructField("root_shell", IntegerType(), True),
#         StructField("su_attempted", IntegerType(), True),
#         StructField("num_root", IntegerType(), True),
#         StructField("num_file_creations", IntegerType(), True),
#         StructField("num_shells", IntegerType(), True),
#         StructField("num_access_files", IntegerType(), True),
#         StructField("num_outbound_cmds", IntegerType(), True),
#         StructField("is_host_login", IntegerType(), True),
#         StructField("is_guest_login", IntegerType(), True),
#         StructField("count", IntegerType(), True),
#         StructField("srv_count", IntegerType(), True),
#         StructField("serror_rate", FloatType(), True),
#         StructField("srv_serror_rate", FloatType(), True),
#         StructField("rerror_rate", FloatType(), True),
#         StructField("srv_rerror_rate", FloatType(), True),
#         StructField("same_srv_rate", FloatType(), True),
#         StructField("diff_srv_rate", FloatType(), True),
#         StructField("srv_diff_host_rate", FloatType(), True),
#         StructField("dst_host_count", IntegerType(), True),
#         StructField("dst_host_srv_count", IntegerType(), True),
#         StructField("dst_host_same_srv_rate", FloatType(), True),
#         StructField("dst_host_diff_srv_rate", FloatType(), True),
#         StructField("dst_host_same_src_port_rate", FloatType(), True),
#         StructField("dst_host_srv_diff_host_rate", FloatType(), True),
#         StructField("dst_host_serror_rate", FloatType(), True),
#         StructField("dst_host_srv_serror_rate", FloatType(), True),
#         StructField("dst_host_rerror_rate", FloatType(), True),
#         StructField("dst_host_srv_rerror_rate", FloatType(), True)
#     ])
#
#     try:
#         stream_df = (spark.readStream
#                      .format("socket")
#                      .option("host", "spark-master")  # Should match your service name
#                      .option("port", 9999)
#                      .load())
#
#         parsed_df = stream_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
#
#         # Select only numeric features for the model
#         feature_cols = [field.name for field in schema if isinstance(field.dataType, (IntegerType, FloatType))]
#         feature_df = parsed_df.select(*feature_cols)
#
#         # Add 'is_anomaly' column
#         processed_df = parsed_df.withColumn("is_anomaly", predict_anomaly_udf(*[col(c) for c in feature_cols]))
#
#         # 👇 Print to console (for dev/debugging)
#         parsed_df.writeStream \
#             .outputMode("append") \
#             .format("console") \
#             .option("truncate", False) \
#             .start()
#
#         # Send to Kafka
#         kafka_df = processed_df.selectExpr(
#             "CAST(duration AS STRING) AS key",
#             "to_json(struct(*)) AS value"
#         )
#
#
#
#         query = (kafka_df.writeStream
#                  .format("kafka")
#                  .option("kafka.bootstrap.servers", config["kafka"]["bootstrap.servers"])
#                  .option("topic", topic)
#                  .option("checkpointLocation", "/tmp/spark-kafka-checkpoint")
#                  .outputMode("append")
#                  .start())
#
#         query.awaitTermination()
#
#     except Exception as e:
#         print(f"Error: {e}")
#
#
# if __name__ == "__main__":
#     spark_conn = SparkSession.builder \
#         .appName("LocalKafkaStreaming") \
#         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
#         .getOrCreate()
#
#     start_streaming(spark_conn)

#
#
# from pyspark.sql import SparkSession
# from pyspark.sql.types import *
# from pyspark.sql.functions import from_json, col, to_json, struct
# import pickle
# import pandas as pd
# import joblib
#
# # Local Kafka config
# config = {
#     "kafka": {
#         "bootstrap.servers": "kafka:9092"
#     }
# }
# model_path = "/opt/bitnami/spark/jobs/isolation_forest.pkl"
# # Define schema for incoming data
# schema = StructType([
#     StructField("duration", IntegerType(), True),
#     StructField("protocol_type", StringType(), True),
#     StructField("service", StringType(), True),
#     StructField("flag", StringType(), True),
#     StructField("src_bytes", IntegerType(), True),
#     StructField("dst_bytes", IntegerType(), True),
#     StructField("land", IntegerType(), True),
#     StructField("wrong_fragment", IntegerType(), True),
#     StructField("urgent", IntegerType(), True),
#     StructField("hot", IntegerType(), True),
#     StructField("num_failed_logins", IntegerType(), True),
#     StructField("logged_in", IntegerType(), True),
#     StructField("num_compromised", IntegerType(), True),
#     StructField("root_shell", IntegerType(), True),
#     StructField("su_attempted", IntegerType(), True),
#     StructField("num_root", IntegerType(), True),
#     StructField("num_file_creations", IntegerType(), True),
#     StructField("num_shells", IntegerType(), True),
#     StructField("num_access_files", IntegerType(), True),
#     StructField("num_outbound_cmds", IntegerType(), True),
#     StructField("is_host_login", IntegerType(), True),
#     StructField("is_guest_login", IntegerType(), True),
#     StructField("count", IntegerType(), True),
#     StructField("srv_count", IntegerType(), True),
#     StructField("serror_rate", FloatType(), True),
#     StructField("srv_serror_rate", FloatType(), True),
#     StructField("rerror_rate", FloatType(), True),
#     StructField("srv_rerror_rate", FloatType(), True),
#     StructField("same_srv_rate", FloatType(), True),
#     StructField("diff_srv_rate", FloatType(), True),
#     StructField("srv_diff_host_rate", FloatType(), True),
#     StructField("dst_host_count", IntegerType(), True),
#     StructField("dst_host_srv_count", IntegerType(), True),
#     StructField("dst_host_same_srv_rate", FloatType(), True),
#     StructField("dst_host_diff_srv_rate", FloatType(), True),
#     StructField("dst_host_same_src_port_rate", FloatType(), True),
#     StructField("dst_host_srv_diff_host_rate", FloatType(), True),
#     StructField("dst_host_serror_rate", FloatType(), True),
#     StructField("dst_host_srv_serror_rate", FloatType(), True),
#     StructField("dst_host_rerror_rate", FloatType(), True),
#     StructField("dst_host_srv_rerror_rate", FloatType(), True)
# ])
#
# def start_streaming(spark):
#     from pyspark.sql.functions import pandas_udf  # Must import after SparkSession
#
#     # Load pre-trained Isolation Forest model
#     # with open("/opt/bitnami/spark/jobs/isolation_forest.pkl", "rb") as f:
#     #     isolation_model = pickle.load(f)
#     isolation_model = joblib.load(model_path)
#
#     print("Loaded model type:", type(isolation_model))
#
#     feature_cols = [field.name for field in schema if isinstance(field.dataType, (IntegerType, FloatType))]
#
#     # Define the UDF after Spark is ready
#     @pandas_udf("integer")
#     def predict_anomaly_udf(*cols):
#         df = pd.concat(cols, axis=1)
#         df.columns = feature_cols
#         preds = isolation_model.predict(df)
#         return pd.Series([1 if x == -1 else 0 for x in preds])
#
#     topic = "network1_topic"
#
#     try:
#         # Read streaming data (from socket or Kafka)
#         stream_df = spark.readStream \
#             .format("socket") \
#             .option("host", "spark-master") \
#             .option("port", 9999) \
#             .load()
#
#         # Parse the incoming JSON string into columns
#         parsed_df = stream_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
#
#         # Select numeric features only
#         # feature_cols = [field.name for field in schema if isinstance(field.dataType, (IntegerType, FloatType))]
#
#         # Add anomaly prediction column
#         processed_df = parsed_df.withColumn("is_anomaly", predict_anomaly_udf(*[col(c) for c in feature_cols]))
#
#         # Optional: Print to console for debugging
#         console_query = processed_df.writeStream \
#             .outputMode("append") \
#             .format("console") \
#             .option("truncate", False) \
#             .start()
#
#         # Convert to Kafka-compatible DataFrame
#         kafka_df = processed_df.selectExpr(
#             "CAST(duration AS STRING) AS key",
#             "to_json(struct(*)) AS value"
#         )
#
#         # ✅ Start Kafka sink and await its termination
#         kafka_query = kafka_df.writeStream \
#             .format("kafka") \
#             .option("kafka.bootstrap.servers", config["kafka"]["bootstrap.servers"]) \
#             .option("topic", topic) \
#             .option("checkpointLocation", "/tmp/spark-kafka-checkpoint") \
#             .outputMode("append") \
#             .start()
#
#         console_query.awaitTermination()
#         kafka_query.awaitTermination()
#
#     except Exception as e:
#         print(f"Error: {e}")
#
# if __name__ == "__main__":
#     spark_conn = SparkSession.builder \
#         .appName("LocalKafkaStreaming") \
#         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
#         .getOrCreate()
#
#     start_streaming(spark_conn)


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, to_json, struct
import pandas as pd
import joblib

# Local Kafka config
config = {
    "kafka": {
        "bootstrap.servers": "kafka:9092"
    }
}
model_path = "/opt/bitnami/spark/jobs/isolation_pipeline.pkl"

# Define schema for incoming data (same as your schema)
schema = StructType([
    StructField("duration", IntegerType(), True),
    StructField("protocol_type", StringType(), True),
    StructField("service", StringType(), True),
    StructField("flag", StringType(), True),
    StructField("src_bytes", IntegerType(), True),
    StructField("dst_bytes", IntegerType(), True),
    StructField("land", IntegerType(), True),
    StructField("wrong_fragment", IntegerType(), True),
    StructField("urgent", IntegerType(), True),
    StructField("hot", IntegerType(), True),
    StructField("num_failed_logins", IntegerType(), True),
    StructField("logged_in", IntegerType(), True),
    StructField("num_compromised", IntegerType(), True),
    StructField("root_shell", IntegerType(), True),
    StructField("su_attempted", IntegerType(), True),
    StructField("num_root", IntegerType(), True),
    StructField("num_file_creations", IntegerType(), True),
    StructField("num_shells", IntegerType(), True),
    StructField("num_access_files", IntegerType(), True),
    StructField("num_outbound_cmds", IntegerType(), True),
    StructField("is_host_login", IntegerType(), True),
    StructField("is_guest_login", IntegerType(), True),
    StructField("count", IntegerType(), True),
    StructField("srv_count", IntegerType(), True),
    StructField("serror_rate", FloatType(), True),
    StructField("srv_serror_rate", FloatType(), True),
    StructField("rerror_rate", FloatType(), True),
    StructField("srv_rerror_rate", FloatType(), True),
    StructField("same_srv_rate", FloatType(), True),
    StructField("diff_srv_rate", FloatType(), True),
    StructField("srv_diff_host_rate", FloatType(), True),
    StructField("dst_host_count", IntegerType(), True),
    StructField("dst_host_srv_count", IntegerType(), True),
    StructField("dst_host_same_srv_rate", FloatType(), True),
    StructField("dst_host_diff_srv_rate", FloatType(), True),
    StructField("dst_host_same_src_port_rate", FloatType(), True),
    StructField("dst_host_srv_diff_host_rate", FloatType(), True),
    StructField("dst_host_serror_rate", FloatType(), True),
    StructField("dst_host_srv_serror_rate", FloatType(), True),
    StructField("dst_host_rerror_rate", FloatType(), True),
    StructField("dst_host_srv_rerror_rate", FloatType(), True)
])

def start_streaming(spark):
    from pyspark.sql.functions import pandas_udf

    # Load pre-trained model
    isolation_model = joblib.load(model_path)
    print("Model loaded:", type(isolation_model))

    # Extract numeric columns for prediction
    # feature_cols = [field.name for field in schema if isinstance(field.dataType, (IntegerType, FloatType))]
    feature_cols = [field.name for field in schema]

    @pandas_udf("integer")
    def predict_anomaly_udf(*cols):
        df = pd.concat(cols, axis=1)
        df.columns = feature_cols
        preds = isolation_model.predict(df)
        return pd.Series([1 if x == -1 else 0 for x in preds])

    topic = "network5_topic"

    try:
        stream_df = spark.readStream \
            .format("socket") \
            .option("host", "spark-master") \
            .option("port", 9999) \
            .load()

        # Parse each incoming JSON line into structured format
        parsed_df = stream_df \
            .select(from_json(col("value"), schema).alias("data")) \
            .filter(col("data").isNotNull()) \
            .select("data.*")

        # Predict anomaly
        processed_df = parsed_df.withColumn("is_anomaly", predict_anomaly_udf(*[col(c) for c in feature_cols]))

        # Kafka: serialize entire row including is_anomaly
        kafka_df = processed_df.select(
            col("duration").cast("string").alias("key"),
            to_json(struct(*processed_df.columns)).alias("value")
        )



        # Console debugging
        console_query = processed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start()



        # Kafka sink
        kafka_query = kafka_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config["kafka"]["bootstrap.servers"]) \
            .option("topic", topic) \
            .option("checkpointLocation", "/opt/bitnami/spark/tmp/kafka-checkpoint-network3") \
            .outputMode("append") \
            .start()

        print("Kafka query started: ", kafka_query.id)

        console_query.awaitTermination()
        kafka_query.awaitTermination()
        print("Kafka sink query status:", kafka_query.status)
        print("Kafka sink query is active:", kafka_query.isActive)


    except Exception as e:
        print(f"Streaming error: {e}")

if __name__ == "__main__":
    spark_conn = SparkSession.builder \
        .appName("KafkaAnomalyStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
        .getOrCreate()

    start_streaming(spark_conn)


