package hive

import java.sql.{ResultSet, ResultSetMetaData, SQLException}
import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.conf._
import org.apache.hive.jdbc._
import process.{Configs, InterpreterLoader}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object HiveHelper {
    val MAX_COL_WIDTH: Int = 50
    val MAX_ROW_COUNT: Int = 100

    def AUX_JARS: String = {
        (FileUtils.getJarFilesByPath(Configs.UDF_PATH,
            InterpreterLoader.sc.hadoopConfiguration)
            .asScala ++ Set(s"${Configs.HIVE_HOME}/hcatalog/share/hcatalog/hive-hcatalog-core-2.3.0.jar",
            s"${Configs.HIVE_HOME}/lib/hive-exec-2.3.0.jar"))
            .mkString(",")
    }

    //    "file:///usr/local/tncdata/apps/apache-hive-2.3.0-bin/hcatalog/share/hcatalog/hive-hcatalog-core-2.3.0.jar,file:///usr/local/tncdata/apps/apache-hive-2.3.0-bin/lib/hive-exec-2.3.0.jar,file:///usr/local/tncdata/apps/apache-hive-2.3.0-bin/udf/commons-pool2-2.4.2.jar,file:///usr/local/tncdata/apps/apache-hive-2.3.0-bin/udf/data-hive-udfs-0.0.2.jar,file:///usr/local/tncdata/apps/apache-hive-2.3.0-bin/udf/jedis-2.9.0.jar,file:///usr/local/tncdata/apps/apache-hive-2.3.0-bin/udf/jopt-simple-5.0.3.jar,file:///usr/local/tncdata/apps/apache-hive-2.3.0-bin/udf/kafka_2.11-0.11.0.0.jar,file:///usr/local/tncdata/apps/apache-hive-2.3.0-bin/udf/kafka-clients-0.11.0.0.jar,file:///usr/local/tncdata/apps/apache-hive-2.3.0-bin/udf/lz4-1.3.0.jar"

    val HIVE_JDBC_URL_UDF: String = s"${Configs.JDBC_HIVE_URL}?${HiveConf.ConfVars.HIVEAUXJARS.varname}=${AUX_JARS}"

    @throws[SQLException]
    def getConn: HiveConnection = new HiveConnection(HIVE_JDBC_URL_UDF, props)

    private def props: Properties = {
        val props: Properties = new Properties
        props
    }

    implicit val _conn: HiveConnection = getConn

    def metaToStr(md: ResultSetMetaData): String =
        (1 to md.getColumnCount)
            .map(md.getColumnLabel(_))
            .mkString("|\t", "\t|\t", "\t|")

    def toRows(rs: ResultSet, maxCount: Int = 0): ListBuffer[List[Object]] = {
        rs.beforeFirst()
        val md: ResultSetMetaData = rs.getMetaData
        val colIdx: Array[Int] = (1 to md.getColumnCount).toArray
        val rows: ListBuffer[List[Object]] = ListBuffer.empty
        var rowCnt: Int = 0
        while (rs.next() && (maxCount <= 0 || rowCnt <= maxCount)) {
            rowCnt += 1
            rows += colIdx.map(rs.getObject(_)).toList
        }
        rows
    }

    @throws[SQLException]
    def rs(sql: String)(implicit hc: HiveConnection): ResultSet = {
        if (StringUtils.isBlank(sql)) return null
        hc.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(sql)
    }

    def rowsToStr(rows: List[List[Object]]): String = {
        val colIdx: Array[Int] = (0 until rows(0).size).toArray
        val maxWidths: Array[Int] = colIdx.map((colId: Int) => rows.map(_ (colId).toString.length).max)

        rows.map(row => row.map(_.toString)
            .zip(maxWidths)
            .map(sw => StringUtils.leftPad(StringUtils.left(sw._1, sw._2), sw._2)))
            .zipWithIndex
            .map(row_i => row_i._2 :: row_i._1)
            .map(_.mkString("|", "\t|", "\t|"))
            .mkString("\n")
    }

    def rsToStr(rs: ResultSet, extend: Boolean = false): String = metaToStr(rs.getMetaData) + "\n" + rowsToStr(toRows(rs).toList)

    def q(sql: String): Unit = println(rsToStr(rs(sql)))
}