import java.io.FileInputStream
import java.nio.file.Paths
import java.util.Properties

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf

import scala.collection.JavaConverters._

/**
  * Created by fan on 2016/12/15.
  */
object Configs {
    def log: Logger = LogManager.getLogger(Configs.getClass)

    def loadProperties(filename: String): Map[String, String] = {
        val re: Map[String, String] = Map.empty
        if (StringUtils.isEmpty(filename)) return re
        val is: FileInputStream = FileUtils.openInputStream(Paths.get(filename).toFile)
        try {
            val props: Properties = new Properties()
            props.load(is)
            return props.asScala.toMap
        } catch {
            case e: Exception => log.error(s"failed to load $filename", e)
        } finally {
            is.close()
        }
        return re
    }

    @Deprecated
    lazy val TOPICS: Set[String] = KafkaHelper.getTopicsByPatterns(TOPIC_CFGS)

    val DATETIME_FORMAT: String = "yyyy-MM-dd_hh-mm-ss"

    val TOPIC_CFGS: Array[String] = StringUtils.split(System.getProperty("kafka.topics", "event-message"), ',').filter(StringUtils.isNotBlank(_)).map(_.trim)
    //TODO
    val ZOOKEEPER_SERVERS: String = System.getProperty("zk.servers", "TO-ADD")
    val BOOTSTRAP_SERVERS: String = System.getProperty("bootstrap.servers", "TO-ADD")
    val INTERVAL: Int = System.getProperty("stream.interval", "120").toInt
    val CHECK_POINT_DIR: String = System.getProperty("stream.checkpoint", "/tmp/checkpoints/kafka")
    val OFFSET_FLAG: String = System.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val KAFKA_GROUP_ID: String = System.getProperty("group.id", "importer")
    val SCRIPT_PATH: String = System.getProperty("script.path", "/user/somepath/scripts/spark/ProcessorLoader.scala")
    val SPARK_CONFIGS: Map[String, String] = Map("spark.app.name" -> "spark-kafka-processor",
        "spark.streaming.stopGracefullyOnShutdown" -> true.toString,
        /*
        Enables or disables Spark Streaming's internal backpressure mechanism (since 1.5).
        This enables the Spark Streaming to control the receiving rate based on
        the current batch scheduling delays and processing times so that the system receives
        only as fast as the system can process. Internally, this dynamically sets
        the maximum receiving rate of receiversDF. This rate is upper bounded
        by the values spark.streaming.receiver.maxRate and spark.streaming.kafka.maxRatePerPartition
        if they are set (see below).
         */
        "spark.streaming.backpressure.enabled" -> true.toString,
        "spark.streaming.blockInterval" -> INTERVAL.toString,
        "spark.streaming.receiver.writeAheadLog.enable" -> true.toString,
        "spark.sql.catalogImplementation" -> "hive",
        //TODO
        "hive.metastore.uris" -> "thrift://hive-somewhere:9083") ++ loadProperties(System.getProperty("configs", "configs.properties"))


    val SPARK_CONF: SparkConf = new SparkConf().setAll(SPARK_CONFIGS)
    val KAFKA_PARAMS: Map[String, Object] = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> BOOTSTRAP_SERVERS,
        "zookeeper.connect" -> ZOOKEEPER_SERVERS,
        ConsumerConfig.GROUP_ID_CONFIG -> KAFKA_GROUP_ID,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> OFFSET_FLAG,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> System.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000"),
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG -> "2097152",
        "zookeeper.connection.timeout.ms" -> "10000",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName)
}