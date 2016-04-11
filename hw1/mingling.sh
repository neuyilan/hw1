java -cp target/esTest-0.0.1-SNAPSHOT-lib/*:target/esTest-0.0.1-SNAPSHOT.jar weibo.es.origin.ESClient 
 
 
java -cp target/esTest-0.0.1-SNAPSHOT-lib/*:target/esTest-0.0.1-SNAPSHOT.jar weibo.es.jest.ESTest


原生 api  
java -cp target/esTest-0.0.1-SNAPSHOT-lib/*:target/esTest-0.0.1-SNAPSHOT.jar weibo.es.origin.ESClient 1 2000000 500
curl -XDELETE 'http://10.13.56.52:9200/dip-hdfs_index_origin-20160328'

java -cp target/esTest-0.0.1-SNAPSHOT-lib/*:target/esTest-0.0.1-SNAPSHOT.jar:target/classes/* weibo.es.origin.ESOriginTest 1 2000000 500 /user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/2016_03_26/00/www_sinaedgeahsolci14ydn_trafficserver-admin.aer.dip.sina.com.cn_12177-2016_03_26_00-20160326001_00000 dip-hdfs_index_origin-20160328 dip-hdfs_type_origin-20160328




jest 测试
java -cp target/esTest-0.0.1-SNAPSHOT-lib/*:target/esTest-0.0.1-SNAPSHOT.jar weibo.es.jest.ESTest 1 2000000 500

java -cp target/esTest-0.0.1-SNAPSHOT-lib/*:target/esTest-0.0.1-SNAPSHOT.jar:target/classes/* weibo.es.jest.ESJestTest 1 2000000 500 /user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/2016_03_26/00/www_sinaedgeahsolci14ydn_trafficserver-admin.aer.dip.sina.com.cn_12177-2016_03_26_00-20160326001_00000 dip-hdfs_index_jest-20160328 dip-hdfs_type_jest-20160328
curl -XDELETE 'http://10.13.56.52:9200/dip-hdfs_index_jest-20160328'



git提交地址
http://10.77.121.137/dip/es-test.git

git下载地址：
git clone http://10.77.121.137/dip/es-test.git







<!--  线上测试     -->
原生 api 
java -cp target/esTest-0.0.1-SNAPSHOT-lib/*:target/esTest-0.0.1-SNAPSHOT.jar:target/classes/* weibo.es.origin.ESOriginTest 10 2000000 500 /user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/2016_03_24/14/www_sinaedgeahsolci14ydn_trafficserver-admin.aer.dip.sina.com.cn_12177-2016_03_24_14-20160324168_00000 dip-hdfs_index_origin-10-20160328 dip-hdfs_type_origin-10-20160328




jest测试
java -cp target/esTest-0.0.1-SNAPSHOT-lib/*:target/esTest-0.0.1-SNAPSHOT.jar:target/classes/* weibo.es.jest.ESJestTest 1 2000000 500 /user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/2016_03_24/14/www_sinaedgeahsolci14ydn_trafficserver-admin.aer.dip.sina.com.cn_12177-2016_03_24_14-20160324168_00000 dip-hdfs_index_jest-1-20160328 dip-hdfs_type_jest-1-20160328

 <!--  线上测试     -->










