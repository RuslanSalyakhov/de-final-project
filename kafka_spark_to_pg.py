from pyspark.sql import SparkSession
from pyspark.sql import functions as f, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType, FloatType, DecimalType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col, lit
from pyspark.sql import  functions as F

# Kafka hostname:port and credentials
KAFKA_HOST = 'rc1a-e0hj6806vko1nj0r.mdb.yandexcloud.net:9091'
KAFKA_LOGIN = 'producer_consumer'
KAFKA_PASSWORD = 'password'

# Schemas, required for json conversion into structured type for dataframe
transaction_schema = StructType([StructField('operation_id', StringType(), False),
            StructField('account_number_from', IntegerType(), True),
            StructField('account_number_to', IntegerType(), True),
            StructField('currency_code', StringType(), True),
            StructField('country', StringType(), True),
            StructField('status', StringType(), True),
            StructField('transaction_type', StringType(), True),
            StructField('amount', DoubleType(), True),
            StructField('transaction_dt', TimestampType(), False)
            ])

currency_schema = StructType([StructField('date_update', TimestampType(), False),
            StructField('currency_code', StringType(), True),
            StructField('currency_code_with', StringType(), True),
            StructField('currency_with_div', FloatType(), True)])

key_schema = StructType([StructField('object_id', StringType(), False),
            StructField('object_type', StringType(), True),
            StructField('sent_dttm', TimestampType(), False),
            StructField('payload', StringType(), True)])

def spark_init(app_name: str) -> SparkSession:

    # Required libraries from maven
    # Download them using .config option with "spark.jars.packages"
    kafka_lib_id = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
    postgres = "org.postgresql:postgresql:42.5.0"

    # Libraries required for integration with Kafka и PostgreSQL
    spark_jars_packages = ",".join(
            [
            kafka_lib_id, postgres
            ]
        )
    
    # Create spark session on local machine and installing spark_jars_packages from maven 
    spark = SparkSession.builder \
            .appName(str(app_name)) \
            .master("local") \
            .config("spark.jars.packages", spark_jars_packages ) \
            .getOrCreate()

    # Checkpoint directory path
    spark.sparkContext.setCheckpointDir('/lessons/')
    
    return spark

def read_kafka(spark: SparkSession) -> DataFrame:

    df = spark.read.format('kafka') \
            .option('kafka.bootstrap.servers', KAFKA_HOST) \
            .option('kafka.security.protocol', 'SASL_SSL') \
            .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
            .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_LOGIN}" password="{KAFKA_PASSWORD}";') \
            .option('subscribe', 'transaction-service-input') \
            .option('kafka.partition.assignment.strategy', 'org.apache.kafka.clients.consumer.RoundRobinAssignor') \
            .option("startingOffsets", "earliest") \
            .load()

    return df

def read_kafka_stream(spark: SparkSession) -> DataFrame:

    df = spark.readStream.format('kafka') \
            .option('kafka.bootstrap.servers', KAFKA_HOST) \
            .option('kafka.security.protocol', 'SASL_SSL') \
            .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
            .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_LOGIN}" password="{KAFKA_PASSWORD}";') \
            .option('subscribe', 'transaction-service-input') \
            .option('kafka.partition.assignment.strategy', 'org.apache.kafka.clients.consumer.RoundRobinAssignor') \
            .option("startingOffsets", "earliest") \
            .load()

    return df

def write_pg(df: DataFrame, table: str) -> None:

    try:
        df.write \
                .format("jdbc") \
                .mode("append") \
                .option("url", "jdbc:postgresql://localhost:5432/postgres") \
                .option("dbtable", str(table)) \
                .option("driver", "org.postgresql.Driver") \
                .option("user", "jovyan") \
                .option("password", "jovyan") \
                .save()
    
    except:
        print(f"Something went wrong during spark write operation to the table '{table}'")

# When reading stream from Kafka using readSteam since spark cannot write actual stream to Postgres we are splitting it on batches
def foreach_batch_function(df: DataFrame, epoch_id: int, table_name: str = 'default_table_name' ) -> None:
    
  try: 
    df.write \
      .format("jdbc") \
      .mode("append") \
      .option("url", "jdbc:postgresql://localhost:5432/postgres") \
      .option("dbtable", f"{table_name}") \
      .option("driver", "org.postgresql.Driver") \
      .option("user", "jovyan") \
      .option("password", "jovyan") \
      .save()
      
  except:
    print(f"Something went wrong during spark streaming write operation to the table '{table_name}'")

# Example for writing a stream but performace for local spark was worse than for batch processing. 
# Possible reason that splitting stream on multiple batches putting additional overheads on local machine 

def write_pg_stream(df: DataFrame, table: str) -> None:
    query = (df.writeStream \
             .foreachBatch(lambda df,epoch_id: foreach_batch_function(df, epoch_id, table_name = table)) \
             .start())
    query.awaitTermination()



if __name__ == "__main__":

    # Init Spark session
    spark = spark_init('Spark to Kafka')

    # Read batch from Kafka
    df = read_kafka(spark)

    # Convert json field "value" into string type and apply key_schema
    df_value = df.withColumn("json", from_json(col("value").cast("string"), key_schema))

    # Kafka produces two types of messages, splitting them using filter function and select field payload
    currency = df_value.filter(df_value.json.object_type == "CURRENCY").select("json.payload")
    transaction = df_value.filter(df_value.json.object_type == "TRANSACTION").select("json.payload")

    # Apply currency_schema converting json field "payload"
    currency_upd = currency.withColumn("new_data", from_json(col("payload"), currency_schema)).select("new_data.*")
    # Rename currency_with_div field from Kafka to currency_code_div - table column name from Postgres
    currency_upd = currency_upd.withColumnRenamed("currency_with_div","currency_code_div")

    # Apply transaction_schema converting json field "payload"
    transaction_upd = transaction.withColumn("new_data", from_json(col("payload"), transaction_schema)).select("new_data.*")

    # Writing data currency_upd to table ruslan_saliahoff_yandex_ru__staging.сurrencies
    write_pg(currency_upd, "ruslan_saliahoff_yandex_ru__staging.сurrencies")

    # Writing data transaction_upd to table ruslan_saliahoff_yandex_ru__staging.transactions
    write_pg(transaction_upd, "ruslan_saliahoff_yandex_ru__staging.transactions")