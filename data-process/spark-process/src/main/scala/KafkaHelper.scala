import java.util

import Configs._
import org.apache.kafka.clients.consumer.ConsumerRecords
//import InterpreterLoader.sc
import kafka.api._
import kafka.cluster.{Broker, BrokerEndPoint}
import kafka.common.TopicAndPartition
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.{DateFormatUtils => DFU, DateUtils => DU}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka010._
import org.apache.zookeeper.data.Stat

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.collection.mutable.ListBuffer

/**
  * Created by fan on 2016/12/15.
  */
object KafkaHelper {
    def log: Logger = LogManager.getLogger(KafkaHelper.getClass)

    val zc: ZkClient = ZkUtils.createZkClient(ZOOKEEPER_SERVERS, 10000, 10000)

    val zu: ZkUtils = ZkUtils(zc, false)

    val OFFSET_DATETIME_FMT: String = "yy-MM-dd_hh-mm-ss"

    def getLogger: Logger = LogManager.getLogger(KafkaHelper.getClass)

    @Deprecated
    def saveOffsets(offsetRanges: Array[OffsetRange], time: Time, groupId: String): Unit = {
        val _log: Logger = getLogger
        _log.info(s"saving offsets:\n\t ${offsetRanges.mkString("\n")}")

        offsetRanges.foreach((offset: OffsetRange) => {
            val zkOffsetPath: String = s"/spark/${Configs.KAFKA_GROUP_ID}/${groupId}/${offset.topic}/${offset.partition}"
            getLogger.info(s"saving offset:\t$offset on zookeeper at path:\n\t $zkOffsetPath\n")
            zu.updatePersistentPath(zkOffsetPath: String, offset.untilOffset.toString)
        })

        _log.info(s"saved offsets:\n\t ${offsetRanges.mkString("\n")}")
    }

    @Deprecated
    def readLastOffsetsFromZK(groupId: String, topicAndPartitions: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = {
        if (StringUtils.isBlank(groupId) || topicAndPartitions.isEmpty) return Map.empty

        topicAndPartitions.map(tp => {
            val zkOffsetPath: String = s"/spark/${Configs.KAFKA_GROUP_ID}/${groupId}/${tp.topic}/${tp.partition}"
            getLogger.info(s"checking offsets on zookeeper at path:\n\t $zkOffsetPath\n")
            val data: (Option[String], Stat) = zu.readDataMaybeNull(zkOffsetPath)
            (tp, data._1.getOrElse("0").toLong)
        }).toMap
    }

    def getTopicsByPatterns(topicPatterns: Array[String]): Set[String] = {
        zu.getAllTopics.filter(topic => topicPatterns.exists(cfg => topic.matches(cfg))).toSet
    }

    def sliceOffsetRanges(ranges: IndexedSeq[OffsetRange], size: Long): IndexedSeq[IndexedSeq[OffsetRange]] = {
        if (ranges.isEmpty) return Array.empty[IndexedSeq[OffsetRange]]
        if (size < 1) throw new IllegalArgumentException(s"size $size is less than 1")
        return ranges.map(sliceOffsetRange(_, size))
    }

    def sliceOffsetRange(r: OffsetRange, size: Long): Vector[OffsetRange] = {
        if (null == r) throw new IllegalArgumentException(s"OffsetRange is null")
        if (size < 1) throw new IllegalArgumentException(s"size $size is less than 1")

        val indices: Vector[Long] = (r.fromOffset to r.untilOffset by size).toVector
        return indices.zipWithIndex
            .map((valAndIdx: (Long, Int)) => OffsetRange(r.topic,
                r.partition,
                valAndIdx._1,
                if (valAndIdx._2 < indices.length - 1) indices(valAndIdx._2 + 1) else r.untilOffset))
    }

    def getOffsetRangesBetween(topicAndPartitions: Set[TopicAndPartition],
                               begin: Long,
                               end: Long = OffsetRequest.LatestTime): Array[OffsetRange] = {
        if (topicAndPartitions.isEmpty) return Array.empty
        val topicAndPartitionToConsumers: Map[TopicAndPartition, SimpleConsumer] = getConsumersByTopicAndPartitions(topicAndPartitions)

        try {
            return topicAndPartitionToConsumers.map((tpAndConsumer: (TopicAndPartition, SimpleConsumer)) => {
                val (tp: TopicAndPartition, consumer: SimpleConsumer) = tpAndConsumer
                getOffsetRangeBetween(tp, consumer, begin, end)
            }).toArray
        } catch {
            case e: Exception =>
                getLogger.error(s"failed to get OffsetRange", e)
                return Array.empty
        } finally {
            topicAndPartitionToConsumers.values.foreach(_.close())
        }
    }

    def getOffsetRangeBetween(tp: TopicAndPartition,
                              consumer: SimpleConsumer,
                              begin: Long = OffsetRequest.EarliestTime,
                              end: Long = OffsetRequest.LatestTime) = {
        OffsetRange(tp.topic, tp.partition, getOffsetBy(tp, consumer, begin), getOffsetBy(tp, consumer, end))
    }

    /**
      * <pre>
      * be noted, the SimpleConsumer#getOffsetsBefore method
      * can't guarantee accurate offset!
      * refers to <a href="http://cfchou.github.io/blog/2015/04/23/a-closer-look-at-kafka-offsetrequest/">a-closer-look-at-kafka-offsetrequest</a>
      * however, in the latest version of kafka
      * there will be lastTime index
      * refers to <a href="https://issues.apache.org/jira/browse/KAFKA-3163">KAFKA-3163</a>
      * </pre>
      */
    def getOffsetBy(tp: TopicAndPartition, consumer: SimpleConsumer, begin: Long): Long = {
        val offsetReq: OffsetRequest = OffsetRequest(Map((tp, PartitionOffsetRequestInfo(begin, 1))))
        val offsetResp: OffsetResponse = consumer.getOffsetsBefore(offsetReq)
        val offsets: Seq[Long] = offsetResp.partitionErrorAndOffsets(tp).offsets
        return offsets.headOption.getOrElse(0)
    }

    def getConsumersByTopicAndPartitions(topicAndPartitions: Set[TopicAndPartition]): Map[TopicAndPartition, SimpleConsumer] = {
        val topicAndPartitionAndBrokers: Map[TopicAndPartition, Broker] = getLeaders(topicAndPartitions)
        val brokerAndConsumers: Map[Broker, SimpleConsumer] = topicAndPartitionAndBrokers
            .values.map((broker: Broker) => {
            val endPoint: BrokerEndPoint = broker.getBrokerEndPoint(SecurityProtocol.PLAINTEXT)
            (broker, simpleConsumer(endPoint.host, endPoint.port))
        }).toMap
        return topicAndPartitionAndBrokers.mapValues(brokerAndConsumers(_))
    }

    def simpleConsumer(host: String, port: Int): SimpleConsumer = {
        return new SimpleConsumer(host, port, ConsumerConfig.SocketTimeout, ConsumerConfig.SocketBufferSize, KAFKA_GROUP_ID)
    }

    def getLeaders(tps: Set[TopicAndPartition]): Map[TopicAndPartition, Broker] = {
        if (tps.isEmpty) return Map.empty

        val brokers: Seq[Broker] = zu.getAllBrokersInCluster()
        return tps.map((tp: TopicAndPartition) => {
            val leaderAndIsrForPartition: LeaderAndIsr = zu.getLeaderAndIsrForPartition(tp.topic, tp.partition).get
            val broker: Broker = brokers.find(_.id == leaderAndIsrForPartition.leader).get
            (tp, broker)
        }).toMap
    }

    def getOffsetRangesBy(topicAndPartitionToTimes: Map[TopicAndPartition, Long]): Array[OffsetRange] = {
        val topicAndPartitions: Set[TopicAndPartition] = topicAndPartitionToTimes.keySet
        if (topicAndPartitions.isEmpty) return Array.empty
        val topicAndPartitionToConsumers: Map[TopicAndPartition, SimpleConsumer] = getConsumersByTopicAndPartitions(topicAndPartitions)

        try {
            return topicAndPartitionToConsumers.map((tpAndConsumer: (TopicAndPartition, SimpleConsumer)) => {
                val (tp: TopicAndPartition, consumer: SimpleConsumer) = tpAndConsumer
                getOffsetRangeBetween(tp, consumer, topicAndPartitionToTimes(tp))
            }).toArray
        } catch {
            case e: Exception =>
                getLogger.error(s"failed to get OffsetRange", e)
                Array.empty
        } finally {
            topicAndPartitionToConsumers.values.foreach(_.close())
        }
    }

    //    def getKafkaRDD(topics: Set[String], time: Long = OffsetRequest.EarliestTime): RDD[ConsumerRecord[String, String]] = {
    //        val _offsetRanges: Array[OffsetRange] = getOffsetRangesBy(getTopicAndPartitions(topics), time)
    //        return getKafkaRDD(_offsetRanges)
    //    }

    def getTopicAndPartitions(topics: Set[String]): Set[TopicAndPartition] = {
        return zu.getPartitionsForTopics(topics.toSeq)
            .flatMap(topicAndPartitions => topicAndPartitions._2.map(TopicAndPartition(topicAndPartitions._1, _)))
            .toSet
    }

    def getOffsetRangesBy(topicAndPartitions: Set[TopicAndPartition],
                          time: Long = OffsetRequest.LatestTime): Array[OffsetRange] = {
        val latestOffsets: Map[TopicAndPartition, Long] = getOffsetsBy(topicAndPartitions)
        return getOffsetsBy(topicAndPartitions, time)
            .map((tpAndOffset: (TopicAndPartition, Long)) => OffsetRange(tpAndOffset._1.topic, tpAndOffset._1.partition, tpAndOffset._2, latestOffsets(tpAndOffset._1))).toArray
    }

    def getOffsetsBy(topicAndPartitions: Set[TopicAndPartition],
                     time: Long = OffsetRequest.LatestTime): Map[TopicAndPartition, Long] = {
        if (topicAndPartitions.isEmpty) return Map.empty
        //TODO, it is poor performance to create connections every time
        val topicAndPartitionToConsumers: Map[TopicAndPartition, SimpleConsumer] = getConsumersByTopicAndPartitions(topicAndPartitions)

        try {
            return topicAndPartitionToConsumers.map((tpAndConsumer: (TopicAndPartition, SimpleConsumer)) => {
                val (tp: TopicAndPartition, consumer: SimpleConsumer) = tpAndConsumer
                (tp, getOffsetBy(tp, consumer, time))
            })
        } catch {
            case e: Exception =>
                getLogger.error(s"failed to get OffsetRange", e)
                Map.empty
        } finally {
            topicAndPartitionToConsumers.values.foreach(_.close())
        }
    }

    //    @deprecated(" bug https://issues.apache.org/jira/browse/SPARK-19680")
    //    def getKafkaRDD(_offsetRanges: Array[OffsetRange]): RDD[ConsumerRecord[String, String]] = {
    //        val kafkaRDD: RDD[ConsumerRecord[String, String]] = KafkaUtils.createRDD[String, String](
    //            sparkContext,
    //            mapAsJavaMapConverter(KAFKA_PARAMS).asJava,
    //            _offsetRanges,
    //            LocationStrategies.PreferFixed(java.util.Collections.emptyMap[TopicPartition, String]()))
    //        kafkaRDD
    //    }

    def kafkaConsumer(): KafkaConsumer[String, String] = {
        import org.apache.kafka.clients.consumer.KafkaConsumer
        val props: java.util.Properties = new java.util.Properties
        props.putAll(mapAsJavaMapConverter(KAFKA_PARAMS).asJava)
        props.put("enable.auto.commit", "true")
        //        props.put("auto.commit.interval.ms", "1000")
        props.put("session.timeout.ms", "30000")
        val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
        consumer
    }

    def getMessages(offsetRange: OffsetRange,
                    consumer: KafkaConsumer[String, String] = kafkaConsumer()): Seq[ConsumerRecord[String, String]] = {

        val recList: ListBuffer[ConsumerRecord[String, String]] = ListBuffer.empty
        if (offsetRange.count() == 0) return recList

        val tp: TopicPartition = offsetRange.topicPartition()
        consumer.assign(util.Arrays.asList(tp))
        consumer.seek(tp, offsetRange.fromOffset)

        val size: Long = offsetRange.count()
        import scala.util.control.Breaks._

        breakable {
            while (recList.isEmpty || recList.size < size) {
                val batch: ConsumerRecords[String, String] = consumer.poll(1000)
                recList ++= batch.asScala
                val logMsg: String = s"$tp\tat ${recList.size} loaded ${batch.count()}, have loaded ${recList.size} in ${offsetRange.fromOffset} to ${offsetRange.untilOffset} from $tp"
                if (batch.isEmpty) {
                    log.warn(logMsg)
                    log.warn(s"at ${consumer.position(tp)} offset in ${offsetRange.fromOffset} to ${offsetRange.untilOffset} from $tp")
                    break
                } else {
                    log.info(logMsg)
                }
            }
        }
        return recList.take(size.toInt)
    }

    def getMessages(offsetRanges: Array[OffsetRange]): Seq[(OffsetRange, Seq[ConsumerRecord[String, String]])] = {
        offsetRanges.par.map(or => {
            val consumer: KafkaConsumer[String, String] = kafkaConsumer()
            try {
                (or, getMessages(or, consumer))
            } finally {
                consumer.close()
            }
        }).seq
    }

    def _date(dateTimeStr: String) = DU.parseDate(dateTimeStr, OFFSET_DATETIME_FMT)
}
