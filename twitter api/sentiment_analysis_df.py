import os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, IntegerType, StructField, StructType
from pyspark.ml.feature import HashingTF, IDF, StringIndexer, Tokenizer, StopWordsRemover
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, NaiveBayes
from nltk.corpus import stopwords
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import pprint


# directory_path = 'file:///home/hduser/twitter_data'
file_path = '/data/twitter_data/z_sample.csv'


def train_model(model_pipeline, train_set, val_set):

    # training the model
    model_pipeline_fitted = model_pipeline.fit(train_set)
    train_set_fitted = model_pipeline_fitted.transform(train_set)
    val_set_fitted = model_pipeline_fitted.transform(val_set)

    # evaluation
    evaluator = BinaryClassificationEvaluator()
    train_auc = evaluator.evaluate(train_set_fitted)
    val_auc = evaluator.evaluate(val_set_fitted)
    train_accuracy = train_set_fitted.filter(train_set_fitted.label == train_set_fitted.prediction).count()\
                     / float(train_set_fitted.count())
    val_accuracy = val_set_fitted.filter(val_set_fitted.label == val_set_fitted.prediction).count()\
                   / float(val_set_fitted.count())
    metrics = {
        'train_auc': train_auc,
        'val_auc': val_auc,
        'train_accuracy': train_accuracy,
        'val_accuracy': val_accuracy
    }
    return model_pipeline_fitted,  metrics


if __name__ == '__main__':

    # create a spark context
    sc = SparkContext.getOrCreate()

    # create a sql spark context
    sql = SQLContext(sc)

    # defining a schema for the data
    schema = StructType([
        StructField('polarity', IntegerType(), True),
        StructField('id', StringType(), True),
        StructField('date', StringType(), True),
        StructField('query', StringType(), True),
        StructField('user', StringType(), True),
        StructField('text', StringType(), True)
    ])
    useless_columns = ['id', 'date', 'query', 'user']

    # reading the data as a DataFrame and turning it into a RDD
    df = sql.read.csv(file_path,
                      schema=schema,
                      header=False,
                      sep=',')

    # dropping useless columns
    for col in useless_columns:
        df = df.drop(col)

    # splitting the data
    (df_train, df_val, df_test) = df.randomSplit(weights=[.6, .2, .2], seed=42)

    # defining regular expressions
    # word_re = '\\w+'
    # address_re = '\\@[a-zA-Z0-9_]*'
    # link_re = 'http(s)?://[a-zA-Z0-9./\\-]*'

    # getting the stop words
    try:
        sw = set(stopwords.words('english'))
    except LookupError:
        import nltk
        nltk.download('stopwords')
    finally:
        sw = set(stopwords.words('english'))
        # we want to keep some of the words
        words_to_keep = {'no', 'not', 'up', 'off', 'down', 'yes'}
        sw = sw.difference(words_to_keep)

    # defining our pipeline
    # tokenizer
    tokenizer = Tokenizer(inputCol='text', outputCol='tokens1')
    # stop words remover
    stop_word_remover = StopWordsRemover(inputCol='tokens1', outputCol='tokens', stopWords=list(sw))
    # TF
    terms_frequency = HashingTF(numFeatures=2**16, inputCol='tokens', outputCol='tf')
    # IDF
    inverse_document_frequency = IDF(inputCol='tf', outputCol='idf', minDocFreq=100)
    # cleaning the labels
    labels = StringIndexer(inputCol='polarity', outputCol='label')
    # Logistic regression
    logistic_regression = LogisticRegression(labelCol='label', featuresCol='idf')
    # Random forest
    random_forest = RandomForestClassifier(labelCol='label', featuresCol='idf')
    # Naive bayes
    naive_bayes = NaiveBayes(labelCol='label', featuresCol='idf')

    # definition of the pipelines
    pipeline_lr = Pipeline(stages=[
        tokenizer,
        stop_word_remover,
        terms_frequency,
        inverse_document_frequency,
        labels,
        logistic_regression]
    )
    pipeline_rf = Pipeline(stages=[
        tokenizer,
        stop_word_remover,
        terms_frequency,
        inverse_document_frequency,
        labels,
        random_forest]
    )
    pipeline_nb = Pipeline(stages=[
        tokenizer,
        stop_word_remover,
        terms_frequency,
        inverse_document_frequency,
        labels,
        naive_bayes]
    )

    # training the models
    pipeline_fitted_lr, metrics_lr = train_model(pipeline_lr, df_train, df_val)
    print('Logistic regression')
    pprint.pprint(metrics_lr)

    # pipeline_fitted_rf, metrics_rf = train_model(pipeline_rf, df_train, df_val)
    # print('Random forest')
    # pprint.pprint(metrics_lr)

    # pipeline_fitted_nb, metrics_nb = train_model(pipeline_nb, df_train, df_val)
    print('Logistic regression')
    pprint.pprint(metrics_lr)
    # print('Naive Bayes')
    # pprint.pprint(metrics_nb)

    # pipeline_fitted.save('./model')
    pipeline_fitted_lr.save('hdfs:///model_lr')
    sc.stop()