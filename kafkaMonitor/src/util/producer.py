# -*- coding: utf-8 -*-
from kafka import KafkaClient, SimpleConsumer
from pojo.kfkIns import kafka_ins
'''
@author: Administrator
'''
    
def get_max_offsets(consumer_group,topic_name):
    print ("conmeiiiiiiiii")
    kfk_ins=kafka_ins()
    kafka =  KafkaClient(kfk_ins.BROKERS_LIST,kfk_ins.CLIENT_ID,kfk_ins.TIME_OUT)
    topic_offset = {}
    consumer = SimpleConsumer(kafka,consumer_group, topic_name,api_version='0.8.2')
    topic_offset[topic_name] = consumer.offsets
    return topic_offset

if __name__ == "__main__":  
    topic="dipnew9"
    group="testGroup"
    print (get_max_offsets(group,topic))
