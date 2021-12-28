# this code is designed to run on the spark cluster
# based on my understanding, the code can be designed for either

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from sklearn.cluster import KMeans


appendage = "AfikChecking"
termination_time = 60*60*3.5
# countries to load
country_list = ["UK", "FR", "GM", "SP", "NO"]
topics = "UK, FR, GM, SP, NO"
format_for_server = "com.microsoft.sqlserver.jdbc.spark"



def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc

print("Program started: " + appendage)
print("time alloted to streaming: " + str(termination_time))

spark, sc = init_spark('ETL')

# schema for importing data from kafka
basic_schema = StructType([StructField('StationId', StringType(), False),
                            StructField('Date', IntegerType(), False),
                            StructField('Variable', StringType(), False),
                            StructField('Value', IntegerType(), False),
                            StructField('M_Flag', StringType(), True),
                            StructField('Q_Flag', StringType(), True),
                            StructField('S_Flag', StringType(), True),
                            StructField('ObsTime', StringType(), True)])

# schema for state of aggregation
parquet_schema = StructType([StructField('StationId', StringType(), True),
                            StructField('maxTemp', IntegerType(), True),
                            StructField('minTemp', IntegerType(), True),
                            StructField('avgPrcp', DoubleType(), True)])

# database information
username = "arieln"
password = "Qwerty12!"
server_name = "jdbc:sqlserver://technionddscourse.database.windows.net:1433"
database_name = "arieln"
url = server_name + ";" + "databaseName=" + database_name + ";"

# Our kafka server
kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'

# date range
min_date = 20160000
max_date = 20210000


station_df = spark.read.text('ghcnd-stations.txt')
station_df = station_df.select(station_df.value.substr(0, 11).alias('StationId'),
    station_df.value.substr(12, 9).cast("float").alias('latitude'),
    station_df.value.substr(22, 9).cast("float").alias('longitude'))\
    .withColumn("country", F.col("StationId").substr(1, 2))\
    .filter(F.col("country").isin(country_list)) \
    .drop(F.col("country"))
print("read stations")

# overwrite folder with empty dataframe for aggregation state
emptyRDD = spark.sparkContext.emptyRDD()
spark.createDataFrame(emptyRDD, parquet_schema)\
    .write.mode("overwrite").parquet("tmpFile.parquet")

output_mode = "append"

filtered_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", topics) \
    .option("maxOffsetsPerTrigger", "500000") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), schema=basic_schema).alias('json')) \
    .select("json.*") \
    .filter((F.col("Date") >= min_date) & (F.col("Date") <= max_date)) \
    .withColumn("Date", F.to_date(F.col("Date").cast("string"), 'yyyyMMdd')) \
    .drop("M_Flag", "S_Flag", "ObsTime") \
    .filter(F.col("Variable").isin(["TMAX", "TMIN", "PRCP"])) \
    .filter(F.col("Q_Flag").isNull()) \
    .drop("Q_Flag")


def writeToServer(df, epoch):
    """
    For each batch,
    1. Write to time_df the percipitation that fell each day
    2. Update the parquet that saves the aggregation state for each Station per year and month
    :param df:
    :param epoch:
    :return:
    """
    df.persist()
    df.filter(F.col("Variable") == "PRCP")\
        .drop("Variable")\
        .withColumnRenamed("Value", "label")\
        .write \
        .format(format_for_server) \
        .mode("append") \
        .option("url", url) \
        .option("dbtable", "timeDF") \
        .option("user", username) \
        .option("password", password) \
        .save()

    # if not empty, then update state table
    if len(df.take(1)) != 0:
        tmax_temp = df.filter(F.col("Variable") == "TMAX").groupby("StationId").max("Value") \
            .withColumnRenamed("max(Value)", "maxTemp")
        tmin_temp = df.filter(F.col("Variable") == "TMIN").groupby("StationId").min("Value") \
            .withColumnRenamed("min(Value)", "minTemp")
        prcp_temp = df.filter(F.col("Variable") == "PRCP").groupBy("StationId").agg(F.avg("Value").alias("avgPrcp"))
        current = tmax_temp.join(tmin_temp, on="StationId").join(prcp_temp, on="StationId")
        current.write.mode("append").parquet("tmpFile.parquet")
    df.unpersist()

# run query
query = filtered_df\
    .writeStream\
    .outputMode(output_mode)\
    .foreachBatch(lambda df, epoch_id: writeToServer(df, epoch_id))\
    .start()

query.awaitTermination(termination_time)
#after finished perform clustering

print("starting clustering")
pdclustering_df = spark.read.parquet("tmpFile.parquet").groupBy("StationId")\
            .agg(F.avg("maxTemp").alias("maxTemp"), F.avg("minTemp").alias("minTemp"), \
            F.avg("avgPrcp").alias("avgPrcp")).toPandas()
print(pdclustering_df.head())
pdclustering_df["cluster_label"] = KMeans(n_clusters = 3).fit_predict(pdclustering_df[["avgPrcp", "minTemp", "maxTemp"]])
clustering_df = spark.createDataFrame(pdclustering_df)
print("finished clustering combining with station_df")
spatial_df = station_df.join(clustering_df, on="StationId")
print("writing spatial df")
spatial_df.write \
        .format(format_for_server) \
        .mode("append") \
        .option("url", url) \
        .option("dbtable", "spatialDF") \
        .option("user", username) \
        .option("password", password) \
        .save()

print("reading time_df")
prcp_df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("dbtable", "timeDF") \
        .option("user", username) \
        .option("password", password).load()

print("joining spatial and time for model")
model_df = prcp_df.join(spatial_df.drop("latitude", "longitude", "maxTemp", "minTemp"), on="StationId", how="inner")\
    .withColumn("month", F.month("Date")).withColumn("year", F.year("Date")).drop("Date")
print("writing end model")
model_df.write \
        .format(format_for_server) \
        .mode("append") \
        .option("url", url) \
        .option("dbtable", "modelDF") \
        .option("user", username) \
        .option("password", password) \
        .save()

print("finished")
