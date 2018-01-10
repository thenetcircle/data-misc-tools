/**
  * Created by fan on 2016/12/15.
  */

import java.util.Date

import Configs._
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

class KafkaProcessor extends ((SparkSession, Any, Time) => Any) {
    val log: Logger = LogManager.getLogger(classOf[KafkaProcessor])

    def sampleKafkaRDD(sc: SparkContext, rdd: RDD[MessageAndMetadata[String, String]], time: Time): Any = {
        val kafkaRdd: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]

        log.info(s"get kafka rdd\n\t by OffsetRanges:\n\t${kafkaRdd.offsetRanges.sortBy(_.topic).mkString("\n\t")}\n")
        val tpAndFirstMsgs = rdd.groupBy(mm => (mm.topic, mm.partition))
            .map(tpAndMessages => (new TopicAndPartition(tpAndMessages._1), tpAndMessages._2.toArray.apply(2)))
            .collect()
        log.info(tpAndFirstMsgs.sortBy(_._1.asTuple).map(tpAndMsg => s"${tpAndMsg._1} \n\t ${tpAndMsg._2.message()}").mkString("\n\t"))
        return null
    }

    var firstTime: Boolean = false

    override def apply(spark: SparkSession, lastResult: Any, lastTime: Time): Any = {
        val sc = spark.sparkContext
        log.info(s"\n\tnow ProcessorLoader is running at ${new Date(lastTime.milliseconds)}\n")
        //        if ( {
        //            val ca: Calendar = Calendar.getInstance()
        //            ca.setTimeInMillis(lastTime.milliseconds)
        //            ca.get(Calendar.MINUTE) != 1
        //        }) return


        //TODO
        val KAFKA_TOPICS: Set[TopicAndPartition] = KafkaHelper.getTopicAndPartitions(Set("TO-ADD"))

        val topicAndPartitions: Set[TopicAndPartition] = KAFKA_TOPICS



        // if this script file is modified, the firstTime flag will be reset to true
        // this logic will be executed again, so it is better to comment it out if it is not needed
        val offsetRanges: Array[OffsetRange] = if (firstTime) {
            val offsetRangesByTime: Array[OffsetRange] = KafkaHelper.getOffsetRangesBy(topicAndPartitions, KafkaHelper._date("2017-08-14_00-00-00").getTime) //TODO double check this!!!!!!!!!!!!
            log.info(s"running KafkaProcessor for first lastTime")
            log.info(s"process starts from offsets \n\t${offsetRangesByTime.mkString("\n\t")}")
            offsetRangesByTime
            //            offsetRangesByZK(topicAndPartitions)
        } else {
            log.info(s"running KafkaProcessor not for first lastTime")
            offsetRangesByZK(topicAndPartitions)
        }

        offsetRanges.foreach(each => log.info(s"\t\n $each \n"))
        val numberOfLogs: Long = offsetRanges.map(or => or.untilOffset - or.fromOffset).sum
        log.info(s"there are $numberOfLogs to process")
        if (numberOfLogs == 0) {
            firstTime = false
            return
        }

        val batchSize: Long = 4000
        val rangeSlices: IndexedSeq[IndexedSeq[OffsetRange]] = KafkaHelper.sliceOffsetRanges(offsetRanges.sortBy(or => (or.topic, or.partition)).toVector, batchSize)

        for (i <- 0 until rangeSlices.maxBy(_.length).length) {
            rangeSlices.filter(_.length > i).foreach(each => log.info(s"\t\n ${each(0).topicPartition()} batch: $i/${each.length}\n"))
            val _offsetRanges: Array[OffsetRange] = rangeSlices.filter(_.length > i).map(_ (i)).toArray
            log.info(s"processing kafka data by \n\t ${_offsetRanges.sortBy(or => (or.topic, or.partition)).mkString("\n\t")}\n\t")

            val messages: Seq[(OffsetRange, Seq[ConsumerRecord[String, String]])] = KafkaHelper.getMessages(_offsetRanges)
            val data: Seq[(String, Int, String, String)] = messages.map(_._2).flatMap(rs => rs).map((cr: ConsumerRecord[String, String]) => (cr.topic(), cr.partition(), cr.key(), cr.value()))
            val loadedOffsetRanges: Seq[OffsetRange] = messages.map(_._1)

            val rawRdd: RDD[String] = sc.parallelize(data.map(_._4), 1)

            log.info(rawRdd.take(100).mkString("\n\t"))
            //            val msgAndMetaRdd: RDD[(String, Int, String, String)] = rawRdd.map((cr: ConsumerRecord[String, String]) => (cr.topic(), cr.partition(), cr.key(), cr.value()))
            //TODO
            val result = ScriptHelper.processTaskScript[RDD[String]](spark, "/xxx/HiveLoader.scala", rawRdd, lastTime)
            //            if (result.isInstanceOf[Throwable]) {
            //                throw result.asInstanceOf[Throwable]
            //            }
            KafkaHelper.saveOffsets(loadedOffsetRanges.toArray, lastTime, KAFKA_GROUP_ID)
        }

        firstTime = false
        null
    }

    private def offsetRangesByZK(topicAndPartitions: Set[TopicAndPartition]) = {
        val offsetsByZK: Map[TopicAndPartition, Long] = KafkaHelper.readLastOffsetsFromZK(KAFKA_GROUP_ID, topicAndPartitions)
        val latestOffsets: Map[TopicAndPartition, Long] = KafkaHelper.getOffsetsBy(topicAndPartitions)

        offsetsByZK.map((tpAndOffset: (TopicAndPartition, Long)) => {
            val (tp: TopicAndPartition, offset: Long) = tpAndOffset
            OffsetRange(tp.topic, tp.partition, offset, latestOffsets(tp))
        }).toArray
    }
}

new KafkaProcessor