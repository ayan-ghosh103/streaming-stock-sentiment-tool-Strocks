from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from time import sleep

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

schema=StructType([
    StructField("price", IntegerType()),
    StructField("volume", IntegerType()),
    StructField("created_at", StringType())
])    

stocks = spark.readStream.format("kafka")\
                          .option("kafka.bootstrap.servers", "localhost:9092")\
                          .option("subscribe", "stocks_demo")\
                          .option("startingOffsets", "earliest")\
                          .load()
                         
split_col = split(stocks["value"], ",")

stocks = stocks\
    .withColumn('Price', split_col.getItem(0))\
    .withColumn('Volume', split_col.getItem(1))

stock_market = stocks\
        .withColumn('Price', col("Price"))\
        .withColumn('Volume', col("Volume"))\

# Start running the query that prints to the console
query = stock_market\
    .writeStream\
    .format('kafka')\
    .option('kafka.bootstrap.servers', 'localhost:9092')\
    .option('topic', 'stocks_demo_proc')\
    .option('checkpointLocation', 'checkpointB')\
    .start()

query.awaitTermination()

#query0 = values.writeStream.queryName("tweets_table").format("memory").outputMode("append").start()

#while True:
#    spark.sql("select* from tweets_table").show(5)
#    sleep(5)  