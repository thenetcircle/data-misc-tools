import annotation.ProcDescription
import hive.HiveBeeLine
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

/**
  * this file is placed at path same as
  * entrance.script in ./configs/configs.properties
  * which is also on hdfs
  * Created by fan on 2016/12/19.
  */
@ProcDescription(description = "One ring to rule them all")
class ProcessorLoader extends ((SparkSession, Any) => Any) {
    val log: Logger = LogManager.getLogger("ProcessorLoader.scala")

    override def apply(spark: SparkSession, param: Any): Any = {
        //NOTED, this path is hdfs path
        new HiveBeeLine(interval = "1M")(spark, "/user/tncdata/scripts/hive/test.sql")
    }
}

new ProcessorLoader