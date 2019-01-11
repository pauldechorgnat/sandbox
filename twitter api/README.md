# Twitter API
This folder contain the source code for the project of Spark Streaming project at DataScientest.

## Files
* <b>kafka_consumer_for_twitter.py</b>: script building a very simple Kafka consumer for tweets
* <b>kafka_consumer_for_twitter_wordcount.py</b>: script building a simple Kafka consumer for tweets linked to Spark Streaming and computing a streaming wordcount
* <b>kafka_producer_for_twitter.py</b>: script requesting streaming tweets and publishing them to Kafka 
* <b>kafka_producer_for_yahoo_finance.py</b>: script scraping Yahoo Finance page and publishing the results to Kafka
* <b>most_common_us_words.txt</b>: list of 1000 most common US words
* <b>sentiment_analysis_df.py</b>: script training a logistic regression on tweets for sentiment analysis using the Spark ML API
* <b>sentiment_analysis_rdd.py</b>: script training a logistic regression on tweets for sentiment analysis using the Spark MLlib API
* <b>sentiment_analysis_sampling_utils.py</b>: script used to sample the training set of tweets
* <b>sentiment_analysis_utils.py</b>: attempt to create custom transformers for Spark MLlib API (failed attempt)
* <b>sentiment_analysis_load.py</b>: script used to load a pretrained Spark MLlib Logistic Regression

