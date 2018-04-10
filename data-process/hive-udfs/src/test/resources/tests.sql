add jar commons-pool2-2.4.2.jar hamcrest-core-1.3.jar jopt-simple-5.0.3.jar kafka_2.11-0.11.0.0.jar lz4-1.3.0.jar scala-library-2.11.11.jar slf4j-api-1.6.1.jar zkclient-0.10.jar data-hive-udfs-0.0.2.jar jedis-2.9.0.jar junit-4.12.jar kafka-clients-0.11.0.0.jar metrics-core-2.2.0.jar scala-parser-combinators_2.11-1.0.4.jar snappy-java-1.1.2.6.jar;

delete jar data-hive-udfs-0.0.2.jar;
add jar data-hive-udfs-0.0.2.jar;

drop function if exists http_get;
create function http_get as 'com.thenetcircle.service.data.hive.udf.http.UDFHttpGet';
drop function if exists http_post;
create function http_post as 'com.thenetcircle.service.data.hive.udf.http.UDFHttpPost';

drop function if exists url_encode;
create function url_encode as 'com.thenetcircle.service.data.hive.udf.http.UDFUrlEncoder';

drop function if exists t_http_get;
create function t_http_get as 'com.thenetcircle.service.data.hive.udf.http.UDTFHttpGet';
drop function if exists t_http_post;
create function t_http_post as 'com.thenetcircle.service.data.hive.udf.http.UDTFHttpPost';

drop function if exists zk_read;
create function zk_read as 'com.thenetcircle.service.data.hive.udf.zookeeper.UDFZooKeeperRead';
drop function if exists zk_write;
create function zk_write as 'com.thenetcircle.service.data.hive.udf.zookeeper.UDFZooKeeperWrite';
drop function if exists zk_delete;
create function zk_delete as 'com.thenetcircle.service.data.hive.udf.zookeeper.UDFZooKeeperDelete';
drop function if exists t_zk_write;
create function t_zk_write as 'com.thenetcircle.service.data.hive.udf.zookeeper.UDTFZooKeeperWrite';
drop function if exists t_zk_delete;
create function t_zk_delete as 'com.thenetcircle.service.data.hive.udf.zookeeper.UDTFZooKeeperDelete';

drop function if exists m_add;
create function m_add as 'com.thenetcircle.service.data.hive.udf.commons.UDFMapAdd';

drop function if exists kf_consumer_cfgs;
create function kf_consumer_cfgs as 'com.thenetcircle.service.data.hive.udf.kafka.UDFKafkaDefaultConsumerCfgs';

drop function if exists kf_producer_cfgs;
create function kf_producer_cfgs as 'com.thenetcircle.service.data.hive.udf.kafka.UDFKafkaDefaultProducerCfgs';

drop function if exists kf_topics;
create function kf_topics as 'com.thenetcircle.service.data.hive.udf.kafka.UDFKafkaTopics';

drop function if exists t_kf_push;
create function t_kf_push as 'com.thenetcircle.service.data.hive.udf.kafka.UDTFKafkaPush';

drop function if exists kf_pull;
create function kf_pull as 'com.thenetcircle.service.data.hive.udf.kafka.UDFKafkaPull';


delete jar data-hive-udfs-0.0.2.jar;
add jar data-hive-udfs-0.0.2.jar;
drop function if exists udf_test;
create function udf_test as 'com.thenetcircle.service.data.hive.udf.tests.UDFTest';

drop function if exists gudf_tests;
create function gudf_tests as 'com.thenetcircle.service.data.hive.udf.tests.GenericUDFTests';

drop function if exists udtf_tests;
create function udtf_tests as 'com.thenetcircle.service.data.hive.udf.tests.UDTFTests';


delete jar data-hive-udfs-0.0.2.jar;
add jar data-hive-udfs-0.0.2.jar;
drop function if exists jd_set2;
create function jd_set2 as 'com.thenetcircle.service.data.hive.udf.redis.UDTFJedisSet2';
drop function if exists jd_get;
create function jd_get as 'com.thenetcircle.service.data.hive.udf.redis.UDTFJedisGet';
drop function if exists jd_del;
create function jd_del as 'com.thenetcircle.service.data.hive.udf.redis.UDTFJedisDel';
drop function if exists jd_mget;
create function jd_mget as 'com.thenetcircle.service.data.hive.udf.redis.UDTFJedisMultiGet';
drop function if exists jd_mset;
create function jd_mset as 'com.thenetcircle.service.data.hive.udf.redis.UDTFJedisMultiSet';

!reconnect
delete jar file:///usr/local/tncdata/tmp/data-hive-udfs-0.0.2.jar;
add jar file:///usr/local/tncdata/tmp/data-hive-udfs-0.0.2.jar;
drop function if exists max_with;
create function max_with as 'com.thenetcircle.service.data.hive.udf.commons.UDAFCmpBase';
select max_with(city_id, array(city_id, city)) from gc where region_id='331' group by region_id;

select max_with(city_id, city) from gc where region_id='331' group by region_id;

select region_id, max_with(city_id, struct(city_id, city)) from gc where region_id='323' group by region_id;

