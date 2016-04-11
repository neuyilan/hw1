 
 
 生产者：
 java -cp target/KafkaESTest-0.0.1-SNAPSHOT.jar:target/KafkaESTest-0.0.1-SNAPSHOT-lib/*:target/classes/* weibo.kafka.es.util.KProducer /user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/2016_03_28/00/www_sinaedgeahsolci14ydn_trafficserver-admin.aer.dip.sina.com.cn_12177-2016_03_28_00-20160328001_00000 dipTopic 

消费者：

原生
 java -cp target/KafkaESTest-0.0.1-SNAPSHOT.jar:target/KafkaESTest-0.0.1-SNAPSHOT-lib/*:target/classes/* weibo.kafka.es.origin.KComsumer dip-hdfs_index_origin-20160330 dip-hdfs_type_origin-20160330 10 dipTopic
 
 
jest测试
  java -cp target/KafkaESTest-0.0.1-SNAPSHOT.jar:target/KafkaESTest-0.0.1-SNAPSHOT-lib/*:target/classes/* weibo.kafka.es.jest.JestKComsumer dip-hdfs_index_jest-20160330 dip-hdfs_type_jest-20160330 10 dipTopic
 
t2=337128*8
t1=337128*2

jt3=337128*6
jt4=337128*10

jt5=337128*11

jt6=337128*8;

jt7=337128*4;

 查看主题详细  
bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic test  


nb1=337128*20;        7004010
nb3=337128*10;        3371280*3