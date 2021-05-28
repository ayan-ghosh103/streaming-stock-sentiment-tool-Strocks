from __future__ import print_function
import nltk
import sys
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SparkSession,SQLContext
from pyspark.sql.functions import split, lit, udf
from nltk.sentiment.vader import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')
from time import sleep
import datetime


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

tweets = spark.readStream.format("kafka")\
                          .option("kafka.bootstrap.servers", "localhost:9092")\
                          .option("subscribe", "twitter_demo")\
                          .option("startingOffsets", "earliest")\
                          .load()

tweets = tweets.selectExpr("CAST(value AS STRING)")

split_col = split(tweets["value"], "~")

tweets = tweets\
    .withColumn('Tweet', split_col.getItem(0))\

# Get Sentiment
analyzer=SentimentIntensityAnalyzer()                            
sentiment_analyzer_udf = udf(lambda text: analyzer.polarity_scores(text))
compound_sentiment_udf = udf(lambda score: score.get('compound'))
positive_sentiment_udf = udf(lambda score: score.get('pos'))
negative_sentiment_udf = udf(lambda score: score.get('neg'))
neutral_sentiment_udf = udf(lambda score: score.get('neu'))

tweets = tweets\
    .withColumn('Sentiment', sentiment_analyzer_udf(tweets.Tweet))\

tweets = tweets\
    .withColumn('Positive', positive_sentiment_udf(tweets.Sentiment))\
    .withColumn('Negative', negative_sentiment_udf(tweets.Sentiment))\
    .withColumn('Neutral', neutral_sentiment_udf(tweets.Sentiment))\
    .withColumn('OverallSentiment', compound_sentiment_udf(tweets.Sentiment))\
    .drop('Sentiment')

tweets = tweets\
    .withColumn('key', lit(datetime.datetime.now()))\
    .drop('value')


# Start stream
tweetsStream = tweets\
        .selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value")\
        .writeStream\
        .format('kafka')\
        .option('kafka.bootstrap.servers', 'localhost:9092')\
        .option('topic', 'twitter_demo_proc')\
        .option('checkpointLocation', 'checkpointA')\
        .start()

tweetsStream.awaitTermination()


#query0 = values.writeStream.queryName("tweets_table").format("memory").outputMode("append").start()

#while True:
#    spark.sql("select* from tweets_table").show(5)
#    sleep(5)  