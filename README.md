# data-misc-tools

[![License](https://img.shields.io/github/license/cfcodefans/data-misc-tools.svg)](LICENSE)

This project hosts several tools to help with development using Hive, Spark

To study and apply how to leverage new features of Hive, Spark, Hadoop
for data process, I created and am developing some functions for Hive and spark.


### Requirement
1. Spark 2.2
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

Basically a timer to run spark jobs without compiling, stop, restart spark process.


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
| {"p":"/hbase/hbaseid","v":"?\u0000\u0000\u0000\u0014master:16000S\\?.?7\u0000;PBUF\n$118bca6f-6f49-4713-85f7-337ff4ad6182"} |
| {"p":"/hbase/namespace","v":""}                    |
| {"p":"/hbase/namespace/hbase","v":"?\u0000\u0000\u0000\u0014master:16000?\u000f??J],g\n\u0005hbase"} |
| {"p":"/hbase/namespace/default","v":"?\u0000\u0000\u0000\u0014master:160009????P4?\n\u0007default"} |
| {"p":"/hbase/draining","v":""}                     |
| {"p":"/hbase/recovering-regions","v":""}           |
| {"p":"/hbase/running","v":"?\u0000\u0000\u0000\u0014master:16000????BE?7PBUF\n\u001cTue Aug 01 12:22:46 GMT 2017"} |
| {"p":"/hbase/online-snapshot","v":""}              |
| {"p":"/hbase/online-snapshot/reached","v":""}      |
| {"p":"/hbase/online-snapshot/acquired","v":""}     |
| {"p":"/hbase/online-snapshot/abort","v":""}        |
| {"p":"/hbase/region-in-transition","v":""}         |
| {"p":"/hbase/flush-table-proc","v":""}             |
| {"p":"/hbase/flush-table-proc/reached","v":""}     |
| {"p":"/hbase/flush-table-proc/acquired","v":""}    |
| {"p":"/hbase/flush-table-proc/abort","v":""}       |
| {"p":"/hbase/table-lock","v":""}                   |
| {"p":"/hbase/table-lock/test","v":""}              |
| {"p":"/hbase/table-lock/hbase:namespace","v":""}   |
| {"p":"/hbase/backup-masters","v":""}               |
| {"p":"/hbase/splitWAL","v":""}                     |
| {"p":"/hbase/rs","v":""}                           |
| {"p":"/hbase/replication","v":""}                  |
| {"p":"/hbase/replication/rs","v":""}               |
| {"p":"/hbase/replication/peers","v":""}            |



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
