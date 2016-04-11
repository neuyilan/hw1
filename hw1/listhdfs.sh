

 生产者：
 java -cp target/KafkaESTest-0.0.1-SNAPSHOT.jar:target/KafkaESTest-0.0.1-SNAPSHOT-lib/*:target/classes/* weibo.kafka.es.util.KProducer /user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/2016_03_30/14 list1

消费者：

jest：
 java -cp target/KafkaESTest-0.0.1-SNAPSHOT.jar:target/KafkaESTest-0.0.1-SNAPSHOT-lib/*:target/classes/* weibo.kafka.es.jest.JestKComsumer dip-hdfs_index_jest-20160331 dip-hdfs_type_jest-20160331 10 list1
 
 原生
 java -cp target/KafkaESTest-0.0.1-SNAPSHOT.jar:target/KafkaESTest-0.0.1-SNAPSHOT-lib/*:target/classes/* weibo.kafka.es.origin.OriginKComsumer dip-hdfs_index_origin-20160331 dip-hdfs_type_origin-20160331 10 list1
 
 
 
 curl -XDELETE 'http://10.13.56.52:9200/dip-hdfs_index_jest-1-20160330'