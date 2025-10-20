


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


