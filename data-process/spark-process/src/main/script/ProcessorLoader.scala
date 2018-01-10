import java.util.Date

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Time

/**
  * Created by fan on 2016/12/19.
  */
class ProcessorLoader extends ((SparkSession, Any, Time) => Any) {
    val log: Logger = LogManager.getLogger("ProcessorLoader.scala")

    def classLoaderTree(clzLoader: ClassLoader): String = {
        import scala.collection.mutable.ListBuffer
        val cldList: ListBuffer[ClassLoader] = ListBuffer(clzLoader)
        while (cldList.last.getParent != null || cldList.size > 100) {
            cldList += cldList.last.getParent
        }
        cldList.mkString("\n\t")
    }

    override def apply(spark: SparkSession, param: Any, lastTime: Time): Any = {
        val sc = spark.sparkContext
        log.info(s"\n\tnow ProcessorLoader is running at ${new Date(lastTime.milliseconds)}\n")
        import org.apache.spark.rdd.RDD
        val rdd1: RDD[Int] = spark.sparkContext.parallelize(1 to 1000000)
        println(s"rdd classLoader: \n\t ${classLoaderTree(rdd1.getClass.getClassLoader)}")
        val func: Int => Int = _ * 2 + 1
        println(s"func classLoader: \n\t ${classLoaderTree(func.getClass.getClassLoader)}")
        println(rdd1.map(func).take(100).mkString("\n"))
        return null
    }
}

new ProcessorLoader