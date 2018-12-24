from redis.sentinel import Sentinel
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

import traceback

REDIS_NODES = [('127.0.0.1', 26379),('127.0.0.1', 26380),('127.0.0.1', 26381)]
SOCKET_TIMEOUT = 300
MASTER_POOL_NAME = 'brand_norm_redis'

def get_sentinel_instance():
    if 'SentinelSingletonInstance' not in globals():
        globals()['SentinelSingletonInstance'] = Sentinel(REDIS_NODES, socket_timeout=SOCKET_TIMEOUT)
    return globals()['SentinelSingletonInstance']

def read_offsets_from_redis(sentinel,topics):
    from_offsets={}
    topic_array = topics.split(",")
    master = sentinel.master_for(MASTER_POOL_NAME, socket_timeout=SOCKET_TIMEOUT)
    try:
        for topic in topic_array:
            offset_dict = master.hgetall(topic+"_offset")
            if offset_dict:
                for key in offset_dict:
                    topic_partion = TopicAndPartition(topic, int(key))
                    offset = int(offset_dict[key])
                    from_offsets[topic_partion] = offset
            else:
                print("offsets dict is empty")
    except Exception as e:
        print("fetch offset from redis error:"+str(e))
        print(traceback.format_exc())
        raise Exception("fetch offset from redis error!")
    print("read current offsets:")
    print(from_offsets)
    return from_offsets

def save_offsets_to_redis(rdd):
    sentinel = get_sentinel_instance()
    master = sentinel.master_for('brand_norm_redis', socket_timeout=SOCKET_TIMEOUT)
    offsets = {}
    try:
        for offset in rdd.offsetRanges():
            path = f"{offset.topic}/{offset.partition}"
            print(path)
            print(offset.untilOffset)
            offsets[offset.partition] = offset.untilOffset
            master.hset(offset.topic+'_offset',offset.partition,str(offset.untilOffset).encode())
    except Exception as e:
        print("save offset to redis error:"+str(e))
        print(traceback.format_exc())