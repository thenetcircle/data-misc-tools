# data-misc-tools

[![License](https://img.shields.io/github/license/cfcodefans/data-misc-tools.svg)](LICENSE)

This project hosts several tools to help with development using Hive, Spark

To study and apply how to leverage new features of Hive, Spark, Hadoop
for data process, I created and am developing some functions for Hive and spark.


### Requirement
1. Spark 2.2 (in order to avoid version confliction between Spark and Hive, I recompiled spark without hive support)
2. Hive 2.3
3. Hadoop 3.0.0

## 1. Spark Runner

Spark Runner is inspired by SparkShell, It uses the same Scala Interpreter with SparkSession bound in
Interpreter context, so it can:

1. interpret scala code from a file on hdfs(will extend to load scala/java/python from more source/storage)
2. compile the code into some scala function that takes parameters of current time, sparksession, arbitaray parameters such as some Rdd
3. run and process the data according the timing.

It will run at one minute interval periodically and detect the change of scala source,
change of code will trigger scala interpreter to compile and update the ongoing scala function.

Basically a timer to run spark jobs without compiling, stopping, deploying, restarting spark process.

Add support to run hive sql script files stored on HDFS

I reuse the Hive Beeline instance and replace the input/output stream
so that the spark runner can mimic beeline to run hive sql script, combined with my hive UDF,
Addition to Hive ability to load data from hdfs files, 
It is also capable of:
1. loading data from http
2. sending data to http
3. pulling data from kafka according to time window (require support of [kafka_2.11-0.11.0.0](https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/connect/data/Timestamp.html))
4. push data to kafka
5. read/set key/values to redis

To do, will add support for java/python source file

### 1.1 Simple Scheduler 
Added simple annotation to manager task execution, such as task description, interval to run.
```scala
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import annotation.ProcDescription
import hive._
/**
  * Created by fan on 2018/04/10.
  */
@ProcDescription(description="One ring to rule them all", interval="PT1M")
class ProcessorLoader extends ((SparkSession, Any) => Any) {
    val log: Logger = LogManager.getLogger("ProcessorLoader.scala")
    override def apply(spark: SparkSession, param: Any): Any = {
         log.info("testing....")
         new HiveBeeLine(interval = "PT60M")(spark, "/user/tncdata/scripts/hive/test.sql")
    }
}
new ProcessorLoader
```


## 2. Hive functions

Regarding Hive UDF(User Defined Function), I hope to develop more functions to enable hive to connect/interact with many other data sources(as long as it provides data through some URI). Therefore this can become an option for ETL and different approach to apply machine learning algorithms to data in hive. 

### Some notes (might be wrong/insufficient)

Hive has 3 different types of UDF
1. [GenericUDF](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateFunction) This kind of UDF executes on each row given by sql, applies arbitrary business logic and returns the transformed data. It has initialize method invoked before the first row is passed into evaluate() method, however it does not get notified when the last row is passed. Therefore it is unsuitable for some cases such as using some clients to access data sources like Kafka, since it might not know when to close the client. Otherwise it is expensive to create a kafka client then close it for each row's execution.
  
2. [GenericUDTF](https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide+UDTF) The output of UDTF is treated as a complete table, therefore in select query, the UDTF expression must be the only one term exclusively, it can be nested inside other functions. However its close() method is called. Therefore I develop t_zk_write(udf to write zookeeper) as UDTF, so it can create single zookeeper client and use it to write all the rows to zookeeper, kafka UDTFs are similar cases.


3. [GenericUDAFResolver2](https://cwiki.apache.org/confluence/display/Hive/GenericUDAFCaseStudy) (I am still studying this) 


### HTTP UDF (get/post)
1. url is always string
2. timeout is always int, default by 3000
3. headers is a map<string, string>
4. content is string
 
|type|name|parameters|return|
| --- | --- | --- | --- |
|udf|http_get|_FUNC_(url, timeout, headers) - send get request to url with headers in timeout|http code, headers, content|
|udf|http_post|_FUNC_(url, timeout, headers, content) - send post to url with headers in timeout|http code, headers, content|
|udf|url_encode|_FUNC_(string) - send get request to url with headers in timeout|encoded string|
|udtf|t_http_get|_FUNC_(url, timeout, headers) - send get request to url with headers in timeout|http code, headers, content|
|udtf|t_http_post|_FUNC_(url, timeout, headers, content) - send post to url with headers in timeout|http code, headers, content|

#### Examples
This call of UDF http_get will need to open and close http client instance of each of 10 rows.
```sql
select city_name, http_get(printf("http://solr:8984/solr/user/select?fl=gender,age,nickname&q=city:'%s'&wt=json", city_name)) from p_city limit 10;
```

This call of UDTF t_http_get will only open one http client instance and share with all rows, then close it
```sql
select t_http_get(city_name, printf("http://solr:8984/solr/user/select?fl=gender,age,nickname&q=city:%s&wt=json", url_encode(city_name))) from p_city limit 30;
```
city_name is a context column to mark result of each row.

This one crawls data from git
```sql
select http_get(printf("https://api.github.com/orgs/%s", "apache"), 10000);
```
result is like:

|                        _c0                         |
| -------------------------------------------------- |
| {"code":200,"headers":{"Transfer-Encoding":"chunked","Status":"200 OK","Server":"GitHub.com"...},"content":"{\"login\":\"apache\",\"id\":47359...}"} |

This one using UDTF 
```sql
select t_http_get("apache", printf("https://api.github.com/orgs/%s", "apache"), 10000);
```
gives output like:

| code  |                      headers                       |                      content                       |   ctx   |
| ----- | -------------------------------------------------- | -------------------------------------------------- | ------- |
| 200   | {"Transfer-Encoding":"chunked","Status":"200 OK","Server":"GitHub.com","Access-Control-Allow-Origin":"*"...} | {"login":"apache","id":47359,...} | apache  |



### ZOOKEEPER UDF (read/write/delete, can't watch, sorry)

1. zkAddress is string like host1:port,host2:port...
2. timeout is int
3. pathToRead is string

|type|name|parameters|return|
| --- | --- | --- | --- |
|udf|zk_read|zk_read(String zkAddress, int timeout, String...pathToReads...) recursively read data from zookeeper paths|struct of "p", "v", path and value|
|udf|zk_write|zk_write(String zkAddress, int timeout, Map pathAndValues) recursively write values paired with zookeeper paths|struct of "p", "v", path and old value|
|udf|zk_delete|zk_delete(String zkAddress, int timeout, String...pathToDeletes) recursively delete zookeeper paths|struct of "p", "v", path and old value|
|udtf|t_zk_write|t_zk_write(String zkAddress, int timeout, String...pathAndValues) recursively write values paired with zookeeper paths|struct of "p", "v", path and old value|
|udtf|t_zk_delete|t_zk_delete(Any context, String zkAddress, int timeout, Map<String, String> pathAndValues) recursively write values paired with zookeeper paths|struct of "p", "v", path and old value|

For example, read from path /hbase/:
```sql
select explode(zk_read("some-server:2181,some-server:2181,some-server:2181",3000,"/hbase"));
```
got result like:

| col |
| --- |
| {"p":"/hbase","v":""}                              |
| {"p":"/hbase/table","v":""}                        |
| {"p":"/hbase/table/test","v":"?\u0000\u0000\u0000\u0014master:16000\u0000/<?!??\u0000PBUF\b\u0000"} |
| {"p":"/hbase/table/hbase:namespace","v":"?\u0000\u0000\u0000\u0014master:16000?\u001dA???T?PBUF\b\u0000"} |
| {"p":"/hbase/table/hbase:meta","v":"?\u0000\u0000\u0000\u0014master:16000???\u001e?lP?PBUF\b\u0000"} |
| ... |



### Kafka (/default producer configurations/default consumer configuration/list topics/pull/push)
|type|name|parameters|return|
| --- | --- | --- | --- |
|udf|m_add|m_add(map1, map2....) - Returns a combined map of N maps|a map|
|udf|kf_consumer_cfgs|kf_consumer_cfgs() - return the map containing default settings for kafka consumer|a map|
|udf|kf_producer_cfgs|kf_producer_cfgs() - return the map containing default settings for kafka producer|a map|
|udf|kf_topics|kf_topics() - read all topics from kafka by settings in map parameter (kf_consumer_cfgs)|an array of structs with properties of t(topic) and p(partition)|
|udf|kf_pull|kf_pull(map, start_date_str, end_date_str, topics...) - poll records from kafka by time windows and topics, it returns an array of structs with properties of t(topic), ct(creation time), k(key) and v(value)|an array of structs that has "t" for topic, "ct" for creation time, "k" for key, "v" for value|
|udtf|t_kf_push|t_kf_push(Any context, Map<String, String> map, String topic, String key, String value) - pushWithTransaction records to kafka topics, it returns an array records of structs with properties of t(topic), ct(creation time), k(key) and v(value)|an array of structs that has "t" for topic, "ct" for creation time, "k" for key, "v" for value|


I am working on functions to pull from/push to kafka.

## Example of ETL jobs using Hive SQL with extend UDF
```sql
use mns_tests;

CREATE TEMPORARY MACRO TP() "yyyy-MM-dd'T'HH:mm";
CREATE TEMPORARY MACRO default_now(time_str string) if(time_str IS NOT NULL, unix_timestamp(time_str, TP()), unix_timestamp());
create temporary macro su_strip(s string, t string) reflect('org.apache.commons.lang3.StringUtils', 'strip', s, t);

-- using t_http_get to load data from spark-post web service
-- the result is in form of json
drop table if exists temp_raw_events;
CREATE temporary TABLE temp_raw_events                        
as select t_http_get(
                 named_struct(
                     't', unix_timestamp(),
                     's', from_unixtime(default_now(query_end), TP()),
                     'e', from_unixtime(default_now(query_end)+3600,TP())
                 ),
                 printf('https://api.sparkpost.com/api/v1/message-events?from=%s&to=%s',
                     from_unixtime(default_now(query_end), TP()),
                     from_unixtime(default_now(query_end)+3600,TP())),
                   5000,
                   map('Authorization','spark-post-key',
                         'User-Agent','java-sparkpost/0.19 (eccac9f)',
                         'Content-Type','application/json')) from raw_events;

insert overwrite table raw_events select ctx.t as load_time, ctx.s as query_begin, ctx.e as query_end, content from temp_raw_events;
insert into table history_raw_events select ctx.t as load_time, ctx.s as query_begin, ctx.e as query_end, content from temp_raw_events;

drop table if exists temp_sp_events;
CREATE temporary TABLE temp_sp_events (
ev string
) stored as orc;

--ETL extract, transform, load
insert overwrite table temp_sp_events select evs.ev  from temp_raw_events raw
lateral view explode(
    split(
        su_strip(get_json_object(raw.content, '$.results'), '[]'),
        '(?<=\\}),(?=\\{)')) evs as ev;

--More specific ETL job....
insert into table sp_events (ev, time_stamp, type) 
select ev,
cast(unix_timestamp(substr(get_json_object(ev, '$.timestamp'), 0, 19), "yyyy-MM-dd'T'HH:mm:ss") * 1000 as timestamp)  as time_stamp,
get_json_object(ev, '$.type') as type
from temp_sp_events;
```

