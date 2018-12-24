from kazoo.client import KazooClient
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

import traceback

ZOOKEEPER_SERVERS = "127.0.0.1:2181"

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
            for partition in zk.get_children(f'/consumers/{topic}'):
                topic_partion = TopicAndPartition(topic, int(partition))
                print(topic_partion)
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
        print("save offset to zookeeper error:"+str(e))
        print(traceback.format_exc())