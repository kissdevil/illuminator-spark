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
from kazoo.client import KazooClient

THREDSHOLD = 0.6
INTERVAL = 60
STREAMING_CONSUME_TOPIC_NAME = "cqibrandner"
BROKER_LIST = "127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095"
ZOOKEEPER_SERVERS = "127.0.0.1:2181"

offsetRanges = []

sc = SparkContext.getOrCreate()

sc.addFile('hdfs:///model/crf.model.5Feat_33018Pos_11350Neg')
sc.addFile('hdfs:///data/brand_dict.txt')
sc.addFile('hdfs:///data/part-00001-736e3b80-97f5-41af-b9de-a6c33c55adaa.avro')

spark = SparkSession(sc)

from crfTaggerManager import extract_features
from kafkaProducer import sendData

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
    return pattern.sub(' ', s)


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


def get_zookeeper_instance():
    if 'KazooSingletonInstance' not in globals():
        globals()['KazooSingletonInstance'] = KazooClient(ZOOKEEPER_SERVERS)
        globals()['KazooSingletonInstance'].start()
    return globals()['KazooSingletonInstance']

def read_offsets(zk, topics):
    from pyspark.streaming.kafka import TopicAndPartition
    topic_array = topics.split(",")
    from_offsets={}
    try:
        for topic in topic_array:
            print(topic)
            for partition in zk.get_children(f'/consumers/{topic}'):
                topic_partion = TopicAndPartition(topic, int(partition))
                offset = int(zk.get(f'/consumers/{topic}/{partition}')[0])
                from_offsets[topic_partion] = offset
    except Exception as e:
        print("fetch offset from zookeeper error, will skip this batch:"+str(e))
        print(traceback.format_exc())
    print(from_offsets)
    return from_offsets


def save_offsets(rdd):
    zk = get_zookeeper_instance()
    try:
        for offset in rdd.offsetRanges():
            path = f"/consumers/{offset.topic}/{offset.partition}"
            print(path)
            print(offset.untilOffset)
            zk.ensure_path(path)
            zk.set(path, str(offset.untilOffset).encode())
    except Exception as e:
        print("save offset from zookeeper error:"+str(e))
        print(traceback.format_exc())

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
        finalDf = predict_ner(deseralizedDf).select("itemId","nerBrand","nerBrandInDict")
        finalDf.show(10000, False)
        #finalDf.rdd.foreach(send_msg)
        #calculate_thoughput(python_kafka_producer_performance())
    save_offsets(rdd)

def send_msg(row):
    itemId = row.itemId
    nerBrand = row.nerBrand
    data = {
             'itemId': itemId,
             'nerBrand': nerBrand
           }
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

zk = get_zookeeper_instance()
from_offsets = read_offsets(zk, STREAMING_CONSUME_TOPIC_NAME)

directKafkaStream = KafkaUtils.createDirectStream(
    ssc, [STREAMING_CONSUME_TOPIC_NAME], {"metadata.broker.list": BROKER_LIST},
    fromOffsets=from_offsets)

directKafkaStream.foreachRDD(lambda rdd: process_group(rdd))

ssc.start()
ssc.awaitTermination()
