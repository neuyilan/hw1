# -*- coding: utf-8 -*-  
from kazoo.client import  KazooClient
from pojo.kfkIns import kafka_ins

#zk = KazooClient(hosts="10.13.80.21:2181, 10.13.80.22:2181 ,10.13.80.23:2181/kafka/k1001");
kfk_ins = kafka_ins()
zk=KazooClient(hosts=kfk_ins.ZOOKEEPER_HOSTS);
zk.start()



        

"""
得到consumer目录下面的子目录
"""  
"""得到consumer的group 的id列表"""
def get_groud_ids():
    if(zk.exists(kfk_ins.CONSUMERS)):
        return zk.get_children(kfk_ins.CONSUMERS)
    else:
        print ("the file %s is not exist" % kfk_ins.CONSUMERS)



"""得到某一个具体的consumer group id的 consumer list"""
#/consumers/[group_id]/ids/[consumer_id] --> {"topic1": #streams, ..., "topicN": #streams} (ephemeral node)
def get_consumer_ids(group_id_name):
    path = kfk_ins.CONSUMERS + "/" + group_id_name+"/ids"
    if(zk.exists(path)):
        return zk.get_children(path)
    else:
        print ("the file %s is not exist" % (path) )

#/consumers/[group_id]/owners/[topic]/[broker_id-partition_id] --> consumer_node_id (ephemeral node)        
def get_consumer_owners(group_id_name):
    path = kfk_ins.CONSUMERS + "/" + group_id_name+"/owners"
    if(zk.exists(path)):
        return zk.get_children(path)
    else:
        print ("the file %s is not exist" % (path))
        
#/consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] --> offset_counter_value ((persistent node)
def get_consumer_offsets(group_id_name):
    path = kfk_ins.CONSUMERS + "/" + group_id_name+"/offsets"
    if(zk.exists(path)):
        return zk.get_children(path)
    else:
        print ("the file %s is not exist" % (path))

"""得到某一个consumer下面具体的topic的所有的分区"""
def get_consumer_offsets_topic(group_id_name,topic_name):
    path = kfk_ins.CONSUMERS + "/" + group_id_name+"/offsets/"+topic_name
    if(zk.exists(path)):
        return zk.get_children(path)
    else:
        print ("the file %s is not exist" % (path))

"""得到某一个consumer下面具体的topic的所有的分区            ，      实际上这与get_consumer_offsets_topic方法得到的结果一样"""
def get_consumer_owners_topic(group_id_name,topic_name):
    path = kfk_ins.CONSUMERS + "/" + group_id_name+"/owners/"+topic_name
    if(zk.exists(path)):
        return zk.get_children(path)
    else:
        print ("the file %s is not exist" % (path))

"""得到某一个consumer下面具体的topic的某一个分区"""
def get_consumer_topic_partition(group_id_name,topic_name,partition_id):
    path = kfk_ins.CONSUMERS + "/" + group_id_name+"/offsets/"+topic_name+"/"+partition_id
    print ("zk.get(path)",zk.get(path))
    if(zk.exists(path)):
        temp_arr=zk.get(path)
        if(len(temp_arr)>0):
            return temp_arr[0]
        else:
            print ("thr consumer topic partition result is wrong")
    else:
        print ("the file %s is not exist" % (path))

   

if __name__ == '__main__':
    print ("get_groud_ids------>:    ", get_groud_ids())
    
    cos_group="testGroup"
    topic_temp="dipLogTopic"
    partition_temp="3"
    cos_ins_owner="console-consumer-51930_tinker39-1447038169975-7ef2bce7"
    
    print ("get_consumer_ids------>:    ", get_consumer_ids(cos_group))
    print ("get_consumer_offsets------>:    ", get_consumer_offsets(cos_group))
    print ("get_consumer_owners------>:    ", get_consumer_owners(cos_group))
    print ("get_consumer_offsets_topic------>:    ", get_consumer_offsets_topic(cos_group,topic_temp))
    print ("get_consumer_owners_topic------>:    ", get_consumer_owners_topic(cos_group,topic_temp))
    print ("get_consumer_topic_partition------>:    ", get_consumer_topic_partition(cos_group,topic_temp,partition_temp))
#    
#    
#    path = kfk_ins.CONSUMERS + "/" + cos_group+"/ids/"+cos_ins_owner
#    
#    print (zk.get(path))

    
   
   
   
   
   
   
    
