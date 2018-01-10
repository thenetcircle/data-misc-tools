import java.time.temporal.ChronoUnit
import java.util.{Calendar, Date}

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.streaming.Time


class HiveLoader extends ((SparkSession, RDD[String], Time) => Any) {
    val log: Logger = LogManager.getLogger("HiveLoader.scala")

    override def apply(spark: SparkSession, rdd: RDD[String], lastTime: Time): Any = {
        val sc = spark.sparkContext
        var targetTableName: String = "table_f.acts_orc"
        //        val rdd: RDD[(String, Int, String, String)] = kafkaRdd.map((cr: ConsumerRecord[String, String]) => (cr.topic(), cr.partition(), cr.key(), cr.value()))
        val df: DataFrame = makeActivityDataFrame(spark, rdd, targetTableName)
        df.createOrReplaceTempView("raw")

        val startTime: String = "2017-08-14"
        val switch: Boolean = true
        if (switch) {
            val selectSql = s"select * from raw where provider.id in ('FETD', 'FETC')" + {
                if (startTime.nonEmpty) s" and published > '$startTime'" else ""
            }
            saveActsToHiveTable(spark, _sql(spark, selectSql), targetTableName, "table_fsch_raw")
        } else {
            log.info("not table_f data")
        }

        targetTableName = "table_g.acts_orc"
        if (switch) {
            val selectSql = s"select * from raw where provider.id in ('GAYD', 'table_g')" + {
                if (startTime.nonEmpty) s" and published > '$startTime'" else ""
            }
            saveActsToHiveTable(spark, _sql(spark, selectSql), targetTableName, "table_g_raw")
        } else {
            log.info("not table_g data")
        }

        targetTableName = "table_p.acts_orc"
        if (switch) {
            val selectSql = s"select * from raw where provider.id in ('table_p', 'FCOM')" + {
                if (startTime.nonEmpty) s" and published > '$startTime'" else ""
            }
            saveActsToHiveTable(spark, _sql(spark, selectSql), "table_p.acts_orc", "table_pen_raw")
        } else {
            log.info("not dating data")
        }


        return null
    }

    /*
                saveActsToHiveTable(qc.sql(s"select * from $TEMP_TABLE where (provider.id='table_p' or provider.id='FCOM')"), "table_pen_tests.acts_orc", "table_pen_raw")
            saveActsToHiveTable(qc.sql(s"select * from $TEMP_TABLE where (provider.id='GAYD' or provider.id='table_g')"), "table_g_tests.acts_orc", "table_g_raw")
            saveActsToHiveTable(qc.sql(s"select * from $TEMP_TABLE where (provider.id='FETD' or provider.id='FETC')"), "table_fsch_tests.acts_orc", "table_fsch_raw")
     */

    def tryConcatenatePartitionsForDateRange(spark: SparkSession, tableName: String, start: String, end: String): Unit = {
        val startDate: Date = DateUtils.parseDate(start, "yyyy-MM-dd")
        val endDate: Date = DateUtils.parseDate(end, "yyyy-MM-dd")
        val dates: Array[String] = (0L to startDate.toInstant.until(endDate.toInstant, ChronoUnit.DAYS))
            .map(startDate.toInstant.plus(_, ChronoUnit.DAYS))
            .map(_.toEpochMilli)
            .map(new Date(_))
            .map(DateFormatUtils.format(_, "yyyy-MM-dd")).toArray
        tryConcatenatePartitionsForDates(spark, tableName, dates: _*)
    }

    def tryConcatenatePartitionsForDates(spark: SparkSession, tableName: String, dateStrs: String*): Unit = {
        for (dateStr <- dateStrs) {
            val _date: Date = DateUtils.parseDate(dateStr, "yyyy-MM-dd")
            log.info(s"going to concatenate partitions for table ${tableName} on date ${_date}")
            val hivePartitions: Array[Map[String, String]] = getHivePartitionsBy(spark, tableName, Map("act_date" -> DateFormatUtils.format(_date, "yyyy-MM-dd")))
            concatenatePartitions(spark, tableName: String, hivePartitions)
        }
    }

    def tryConcatenatePartitionsForYesterday(spark: SparkSession, lastTime: Time): Unit = {
        val last: Date = new Date(lastTime.milliseconds)
        val now: Date = new Date(System.currentTimeMillis())

        if (DateUtils.truncatedEquals(now, last, Calendar.DATE)) {
            return
        }
        //TODO concatenate partitions
        {
            val tableName: String = "table_p.acts_orc"
            val hivePartitions: Array[Map[String, String]] = getHivePartitionsBy(spark, tableName, Map("act_date" -> DateFormatUtils.format(last, "yyyy-MM-dd")))
            concatenatePartitions(spark, tableName: String, hivePartitions)
        }

        {
            val tableName: String = "table_g.acts_orc"
            val hivePartitions: Array[Map[String, String]] = getHivePartitionsBy(spark, tableName, Map("act_date" -> DateFormatUtils.format(last, "yyyy-MM-dd")))
            concatenatePartitions(spark, tableName: String, hivePartitions)
        }

        {
            val tableName: String = "table_f.acts_orc"
            val hivePartitions: Array[Map[String, String]] = getHivePartitionsBy(spark, tableName, Map("act_date" -> DateFormatUtils.format(last, "yyyy-MM-dd")))
            concatenatePartitions(spark, tableName: String, hivePartitions)
        }
    }

    def getHivePartitionsBy(spark: SparkSession, tableName: String, partitionConds: Map[String, String] = Map.empty): Array[Map[String, String]] = {
        val partitionsSpec: String = partitionConds.map(kv => s"${kv._1}='${kv._2}'").mkString(", ")
        val showPartitions: String = s"show partitions $tableName" + {
            if (partitionConds.nonEmpty) s" partition(${partitionsSpec})" else ""
        }
        val partitions: DataFrame = _sql(spark, showPartitions)
        return partitions.collect()
            .map(_.getAs[String]("result"))
            .map(_.split('/'))
            .map((kvs: Array[String]) => {
                kvs.map(_.split('='))
                    .map((kv: Array[String]) => (kv(0), kv(1)))
                    .toMap
            })
    }

    def _sql(spark: SparkSession, sql: String): DataFrame = {
        if (StringUtils.isBlank(sql)) return null
        log.info(s"exec:\n\t$sql")
        try {
            return spark.sql(sql)
        } catch {
            case e: Exception =>
                log.error(s"failed to execute sql:\n\t $sql", e)
                return null
        }
    }

    def concatenatePartitions(spark: SparkSession, tableName: String, partitionSpecs: Array[Map[String, String]]): Unit = {
        for (partitionConds <- partitionSpecs) {
            val partitionsSpec: String = partitionConds.map(kv => s"${kv._1}='${kv._2}'").mkString(", ")
            _sql(spark, s"alter table $tableName partition($partitionsSpec) concatenate")
        }
    }

    def makeActivityDataFrame(spark: SparkSession, rdd: RDD[String], schemaSrcTable: String): DataFrame = {
        val raw: RDD[String] = rdd.map(_.replace("objectType", "object_type"))

        log.info(raw.take(100).mkString("\n\t"))

        val tmpPath: String = s"/tmp/${schemaSrcTable}_raw/${DateFormatUtils.format(new Date, "yyyy-MM-dd_hh-mm-ss")}"
        raw.saveAsTextFile(tmpPath)
        val qc: SQLContext = spark.sqlContext

        val schema: StructType = StructType(qc.table(schemaSrcTable).schema.filter(fd => fd.name != "act" && fd.name != "act_date"))
        val df: DataFrame = qc.read.format("json").schema(schema).json(tmpPath)
        return df
    }

    def saveActsToHiveTable(spark: SparkSession, df: DataFrame, tableName: String, tempTableName: String): Any = {
        val qc: SQLContext = spark.sqlContext
        try {
            df.createOrReplaceTempView(tempTableName)
            val insertSql = s"insert into $tableName partition (act, act_date) select *, verb as act, substring(published, 0, 10) as act_date from $tempTableName"
            _sql(spark, insertSql)
            return null
        } catch {
            case e: Exception =>
                log.error(e)
                throw e
        } finally {
            qc.dropTempTable(tempTableName)
        }
    }
}

new HiveLoader