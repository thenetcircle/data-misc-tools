package process

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
  * here keeps the configuration definitions
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

    val PROPS_CFGS: Map[String, String] = loadProperties(System.getProperty("configs", "./configs/configs.properties"))

    val DATETIME_FORMAT: String = "yyyy-MM-dd_hh-mm-ss"
    val ZOOKEEPER_SERVERS: String = PROPS_CFGS.getOrElse("zk.servers", "some zookeeper url")
    val BOOTSTRAP_SERVERS: String = PROPS_CFGS.getOrElse("bootstrap.servers", "some kafka url")
    val INTERVAL: Int = PROPS_CFGS.getOrElse("stream.interval", "120").toInt
    val CHECK_POINT_DIR: String = PROPS_CFGS.getOrElse("stream.checkpoint", "/tmp/checkpoints/kafka")
    val OFFSET_FLAG: String = PROPS_CFGS.getOrElse(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val KAFKA_GROUP_ID: String = PROPS_CFGS.getOrElse("group.id", "importer")

    val ENTRANCE_SCRIPT: String = PROPS_CFGS.getOrElse("entrance.script", "/user/tncdata/scripts/spark/ProcessorLoader.scala")

    val SPARK_CONFIGS: Map[String, String] = Map("spark.app.name" -> "spark-data-processor",
        "spark.streaming.stopGracefullyOnShutdown" -> true.toString,
        /*
        Enables or disables Spark Streaming's internal backpressure mechanism (since 1.5).
        This enables the Spark Streaming to control the receiving rate based on
        the current batch scheduling delays and processing times so that the system receives
        only as fast as the system can records. Internally, this dynamically sets
        the maximum receiving rate of receiversDF. This rate is upper bounded
        by the values spark.streaming.receiver.maxRate and spark.streaming.kafka.maxRatePerPartition
        if they are set (see below).
         */
        "spark.streaming.backpressure.enabled" -> true.toString,
        "spark.streaming.blockInterval" -> INTERVAL.toString,
        "spark.streaming.receiver.writeAheadLog.enable" -> true.toString,
        "spark.sql.catalogImplementation" -> "hive") ++ PROPS_CFGS


    val SPARK_CONF: SparkConf = new SparkConf().setAll(SPARK_CONFIGS)
    val KAFKA_PARAMS: Map[String, Object] = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> BOOTSTRAP_SERVERS,
        "zookeeper.connect" -> ZOOKEEPER_SERVERS,
        ConsumerConfig.GROUP_ID_CONFIG -> KAFKA_GROUP_ID,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> OFFSET_FLAG,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> PROPS_CFGS.getOrElse(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000"),
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG -> "2097152",
        "zookeeper.connection.timeout.ms" -> "10000",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName)

    val UDF_PATH: String = PROPS_CFGS.getOrElse("hive.aux.jars.folder", "file:///usr/local/some folder/apps/apache-hive-2.3.0-bin/udf/")
    val HIVE_HOME: String = System.getenv("HIVE_HOME")
    val JDBC_HIVE_URL: String = PROPS_CFGS.getOrElse("jdbc.hive.url", "jdbc:hive2://some hive server/")

    val RECORDS_JDBC_URL: String =
        Configs.PROPS_CFGS.getOrElse("records.jdbc.url",
            "jdbc:mysql://some mysql:3306/spark_proc?autoReconnect=true&createDatabaseIfNotExist=true&useSSL=false"
        )
    val USERNAME: String = Configs.PROPS_CFGS.getOrElse("records.jdbc.username", "some username")
    val PASSWORD: String = Configs.PROPS_CFGS.getOrElse("records.jdbc.password", "some password")
}