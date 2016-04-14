# -*- coding: utf-8 -*-  
'''
@author: Administrator
'''

# -*- coding: utf-8 -*-  
from kazoo.client import  KazooClient
from pojo.kfkIns import kafka_ins
kfk_ins = kafka_ins()
zk = KazooClient(hosts=kfk_ins.ZOOKEEPER_HOSTS);
zk.start()

"""
得到broker下面所有的ids
""" 
def get_brokers_ids():
    if(zk.exists(kfk_ins.BROKERS_IDS)):
        return zk.get_children(kfk_ins.BROKERS_IDS)
    else:
        print ("the file %s is not exist" % kfk_ins.BROKERS_IDS)

"""
得到broker下面所有的topic
"""        
def get_brokers_topics():
    if(zk.exists(kfk_ins.BROKERS_TOPICS)):
        return zk.get_children(kfk_ins.BROKERS_TOPICS)
    else:
        print ("the file %s is not exist" % kfk_ins.BROKERS_TOPICS)



   

if __name__ == '__main__':
    print ("get_brokers_children------>:    ", get_brokers_ids())
    print ("get_brokers_topics------>:    ", get_brokers_topics())  
    t_path="brokers/topics"
    i_path="brokers/ids"
    print (zk.get(t_path+"/app_dipsinacomkafka12345_wwwedge/partitions/35/state"))
    print (zk.get(i_path+"/1079048028"))
   
   
   
   
   
   
    
