# Twitter API
This folder contain the source code for the project of Spark Streaming project at DataScientest.

## Files
* <b>kafka_consumer_for_twitter.py</b>: script building a very simple Kafka consumer for tweets
* <b>kafka_consumer_for_twitter_wordcount.py</b>: script building a simple Kafka consumer for tweets linked to Spark Streaming and computing a streaming wordcount
* <b>kafka_producer_for_twitter.py</b>: script requesting streaming tweets and publishing them to Kafka 
* <b>solution_tp_batch.py</b>: script training a logistic regression on tweets for sentiment analysis using the Spark MLlib API
* <b>solution_to_streaming.py</b>: script subscribing to a Kafka live feed of tweets, uses a logistic regression to classify tweets and store prediction counts in a hbase table
* <b>tweet_storer.py</b>: script to store a given number of tweets in a json format 