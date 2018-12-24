# coding: utf-8

# In[1]:

import json
import regex
import traceback

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.session import SparkSession
from pyspark import SparkFiles

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

from kafka import KafkaProducer

import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)


THREDSHOLD = 0.6
INTERVAL = 30
STREAMING_CONSUME_TOPIC_NAME = "cqibrandner"
BROKER_LIST = "127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095"

offsetRanges = []

sc = SparkContext.getOrCreate()

sc.addFile('hdfs:///model/crf.model.5Feat_33018Pos_11350Neg')
sc.addFile('hdfs:///data/brand_dict.txt')

spark = SparkSession(sc)

from crfTaggerManager import extract_features
from kafkaProducer import sendData
from redisOffsetManager import get_sentinel_instance,read_offsets_from_redis,save_offsets_to_redis

producer = KafkaProducer(bootstrap_servers=BROKER_LIST)


# remove special characters; normalize synonyms by using current brand dictionary
pattern = regex.compile("[^\p{L}\p{N}'.]")

with open(SparkFiles.get('brand_dict.txt'), 'r') as infile:
    brandDictMap = json.load(infile)


def title_brand_normalize(string, brandDict=brandDictMap):
    stringList = string.lower().split()
    res = trigram(stringList, brandDict)
    return (res)


def trigram(stringList, brandDict):
    if len(stringList) < 3:
        return bigram(stringList, brandDict)
    for i in range(len(stringList) - 2):
        if ' '.join([stringList[i], stringList[i + 1], stringList[i + 2]]) in brandDict:
            # print (' '.join([stringList[i], stringList[i+1], stringList[i+2]]))
            if i == len(stringList) - 3:  # if i is third to last word
                return (bigram(stringList[0:i], brandDict) + ' ' + brandDict[
                    ' '.join([stringList[i], stringList[i + 1], stringList[i + 2]])]).strip()
            else:
                return (bigram(stringList[0:i], brandDict) + ' ' + brandDict[
                    ' '.join([stringList[i], stringList[i + 1], stringList[i + 2]])] + ' ' + trigram(stringList[i + 3:],
                                                                                                     brandDict)).strip()
    return bigram(stringList, brandDict)


def bigram(stringList, brandDict):
    if len(stringList) < 2:
        return unigram(stringList, brandDict)
    for i in range(len(stringList) - 1):
        if ' '.join([stringList[i], stringList[i + 1]]) in brandDict:
            # print (' '.join([stringList[i], stringList[i+1]]))
            if i == len(stringList) - 2:  # if i is the second to last word
                return (unigram(stringList[0:i], brandDict) + ' ' + brandDict[
                    ' '.join([stringList[i], stringList[i + 1]])]).strip()
            else:
                return (unigram(stringList[0:i], brandDict) + ' ' + brandDict[
                    ' '.join([stringList[i], stringList[i + 1]])] + ' ' + bigram(stringList[i + 2:], brandDict)).strip()
    return unigram(stringList, brandDict)


def unigram(stringList, brandDict):
    res = ''
    for s in stringList:
        if s in brandDict:
            res = res + brandDict[s] + ' '
        else:
            res = res + s + ' '
    return (res.strip())


def cleanSpecialChar(s):
    if s:
        return pattern.sub(' ', s)
    else:
        return ''


def get_ner_brand(titleToken, brand_signal, probability):
    terms = titleToken.split()
    ner_brand = ''
    if probability >= THREDSHOLD and any([t == 'b' for t in brand_signal]):
        first_index = brand_signal.index('b')
        ner_brand = terms[first_index]
        while first_index + 1 < len(brand_signal) and brand_signal[first_index + 1] == 'i':
            ner_brand = ner_brand + ' ' + terms[first_index + 1]
            first_index += 1
    return ner_brand


def is_brand_in_dict(brand, brandDict=brandDictMap):
    if not brand.strip():
        return False
    else:
        return brand in brandDict


schema = StructType(
    [
        StructField('itemId', LongType(), True),
        StructField('productId', LongType(), True),
        StructField('prevCqiBrandId', LongType(), True),
        StructField('title', StringType(), True),
        StructField('originalBrand', StringType(), True),
        StructField('originalCategories', StringType(), True),
        StructField('manufacturer', StringType(), True),
        StructField('timestamp', LongType(), True),
        StructField('txId', StringType(), True),
        StructField('predictCategoryDepth4', LongType(), True),
        StructField('predictCategoryDepth3', LongType(), True),
        StructField('predictCategoryDepth2', LongType(), True),
        StructField('predictCategoryDepth1', LongType(), True)
    ]
)

def process_group(rdd):
    #processed_rdd = rdd.foreach(process)
    if rdd.count() > 0:
        print("rdd has " + str(rdd.getNumPartitions()) + " partitions")
        df = rdd.toDF()
        deseralizedDf = df.withColumn("data", from_json("_2", schema)).select(col('data.*'))
        finalDf = predict_ner(deseralizedDf)
        #finalDf.rdd.foreach(send_msg)
        finalDf.show(10000,False)
        #calculate_thoughput(python_kafka_producer_performance())
    save_offsets_to_redis(rdd)

def send_msg(row):
    itemId = row.itemId
    productId = row.productId
    prevCqiBrandId = row.prevCqiBrandId
    title = row.title
    originalBrand = row.originalBrand
    originalCategories = row.originalCategories
    manufacturer = row.manufacturer
    timestamp = row.timestamp
    txId = row.txId
    predictCategoryDepth4 = row.predictCategoryDepth4
    predictCategoryDepth3 = row.predictCategoryDepth3
    predictCategoryDepth2 = row.predictCategoryDepth2
    predictCategoryDepth1 = row.predictCategoryDepth1
    nerBrand = row.nerBrand
    nerBrandInDict = row.nerBrandInDict
    data = {
        'itemId': itemId,
        'productId' : productId,
        'prevCqiBrandId': prevCqiBrandId,
        'title':title,
        'originalBrand':originalBrand,
        'originalCategories':originalCategories,
        'manufacturer':manufacturer,
        'timestamp':timestamp,
        'txId':txId,
        'predictCategoryDepth4':predictCategoryDepth4,
        'predictCategoryDepth3':predictCategoryDepth3,
        'predictCategoryDepth2':predictCategoryDepth2,
        'predictCategoryDepth1':predictCategoryDepth1,
    }
    if nerBrandInDict:
        data['nerBrand'] = nerBrand
    else:
        data['nerBrand'] = ''
    sendData(data)

def predict_ner(dataDFRaw):
    clean_func = udf(cleanSpecialChar, StringType())
    dataDFRaw = dataDFRaw.withColumn('productNameToken', clean_func('title'))

    norm_func = udf(title_brand_normalize, StringType())
    dataDFRaw = dataDFRaw.withColumn('productNameTokenNorm', norm_func('productNameToken'))

    schema = StructType([
        StructField('brand_signal', ArrayType(StringType())),
        StructField('probability', FloatType())
    ])
    udf_tagger_feature = udf(extract_features, schema)
    tokenWithFeatureData = dataDFRaw.withColumn("feature", udf_tagger_feature('productNameTokenNorm'))

    udf_ner_brand = udf(get_ner_brand, StringType())
    ner_brand_df = tokenWithFeatureData.withColumn('nerBrand', udf_ner_brand('productNameTokenNorm',
                      'feature.brand_signal','feature.probability'))
    udf_in_dict = udf(is_brand_in_dict, BooleanType())
    ner_brand_df = ner_brand_df.withColumn('nerBrandInDict', udf_in_dict('nerBrand'))
    return ner_brand_df


spark = SparkSession(sc)
ssc = StreamingContext(sc, int(INTERVAL))

#zk = get_zookeeper_instance()
#from_offsets = read_offsets(zk, STREAMING_CONSUME_TOPIC_NAME)

sentinel = get_sentinel_instance()
from_offsets_redis = read_offsets_from_redis(sentinel,STREAMING_CONSUME_TOPIC_NAME)

directKafkaStream = KafkaUtils.createDirectStream(
    ssc, [STREAMING_CONSUME_TOPIC_NAME], {"metadata.broker.list": BROKER_LIST},
    fromOffsets=from_offsets_redis)

directKafkaStream.foreachRDD(lambda rdd: process_group(rdd))

ssc.start()
ssc.awaitTermination()
