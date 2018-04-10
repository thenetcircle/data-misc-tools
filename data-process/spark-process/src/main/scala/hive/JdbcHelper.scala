package hive

import java.sql.{Connection, ResultSet, ResultSetMetaData, SQLException, Statement}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.{JDBC_TABLE_NAME, JDBC_URL}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import process.InterpreterLoader.{sc, spark}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object JdbcHelper extends Serializable {

    def jdbcConnectionFactory(jdbcUrl: String): () => Connection = {
        val jdbcOpt: JDBCOptions = new JDBCOptions(Map(JDBC_URL -> jdbcUrl, JDBC_TABLE_NAME -> ""))
        JdbcUtils.createConnectionFactory(jdbcOpt)
    }

    def jdbcConnection(jdbcUrl: String): Connection = jdbcConnectionFactory(jdbcUrl)()

    def execUpdate(jdbcUrl: String, sql: String): Int = {
        if (StringUtils.isBlank(jdbcUrl)) throw new IllegalArgumentException(s"jdbcUrl is blank:\n\t$jdbcUrl")
        if (StringUtils.isBlank(sql)) throw new IllegalArgumentException(s"sql is blank:\n\t$sql")

        val conn: Connection = jdbcConnection(jdbcUrl)
        try {
            conn.setSavepoint()
            val stat: Statement = conn.createStatement()
            stat.executeUpdate(sql)
        } catch {
            case e: Exception =>
                e.printStackTrace()
                conn.rollback()
                0
        } finally {
            Option(conn).foreach(_.close())
        }
    }

    def jdbcToDF(jdbcUrl: String,
                 sql: String,
                 upperBound: Long,
                 lowerBound: Long,
                 tableName: String = ""): DataFrame = {
        val jdbcOpt: JDBCOptions = new JDBCOptions(Map(JDBC_URL -> jdbcUrl, JDBC_TABLE_NAME -> tableName))
        val connFactory: () => Connection = JdbcUtils.createConnectionFactory(jdbcOpt)

        val rowRdd: JdbcRDD[Row] = new JdbcRDD(sc,
            connFactory,
            sql,
            upperBound,
            lowerBound,
            1,
            new ResultSetToRowMapper(jdbcOpt)) //TODO doesn't work with HIVE_JDBC, fuck!

        val rowList: List[Row] = rowRdd.collect().toList
        return spark.createDataFrame(rowList.asJava, rowList.head.schema)
    }

    class ResultSetToRowMapper(jdbcOpt: JDBCOptions) extends Function[ResultSet, Row] with Serializable {
        var schema: StructType = _

        override def apply(rs: ResultSet): Row = {
            if (schema == null) schema = JdbcUtils.getSchema(rs, JdbcDialects.get(jdbcOpt.url))
            return new GenericRowWithSchema(JdbcRDD.resultSetToObjectArray(rs).map(_.asInstanceOf[Any]), schema)
        }
    }

    def rowsToDF(schema: StructType, rowList: ListBuffer[Row], sps: SparkSession): DataFrame = {
        sps.createDataFrame(rowList.asJava, schema)
    }

    def hiveToRows(jdbcUrl: String, sql: String, tableName: String): (StructType, ListBuffer[Row]) = {
        val jdbcOpt: JDBCOptions = new JDBCOptions(Map(JDBC_URL -> jdbcUrl, JDBC_TABLE_NAME -> tableName))
        val connFactory: () => Connection = JdbcUtils.createConnectionFactory(jdbcOpt)
        val conn: Connection = connFactory()
        var rs: ResultSet = null
        val rowList: ListBuffer[Row] = ListBuffer.empty
        try {
            val stmt: Statement = conn.createStatement()
            rs = stmt.executeQuery(sql)
            val schema: StructType = JdbcUtils.getSchema(rs, JdbcDialects.get(jdbcOpt.url))
            while (rs.next()) {
                rowList += new GenericRowWithSchema(JdbcRDD.resultSetToObjectArray(rs).map(_.asInstanceOf[Any]), schema)
            }
            (schema, rowList)
        } finally {
            println(s"closing rs: $rs")
            Option(rs).foreach(_.close())
            println(s"closing conn: $conn")
            Option(conn).foreach(_.close())
        }
    }

    def getSchema(jdbcUrl: String, tableName: String, sql: String = ""): StructType = {
        val jdbcOpt: JDBCOptions = new JDBCOptions(Map(JDBC_URL -> jdbcUrl, JDBC_TABLE_NAME -> tableName))
        val connFactory: () => Connection = JdbcUtils.createConnectionFactory(jdbcOpt)
        val conn: Connection = connFactory()
        var rs: ResultSet = null
        val _sql: String = if (StringUtils.isBlank(sql)) s"select * from $tableName where false" else sql
        try {
            val stmt: Statement = conn.createStatement()
            rs = stmt.executeQuery(_sql)
            JdbcUtils.getSchema(rs, JdbcDialects.get(jdbcOpt.url))
        } finally {
            println(s"closing rs: $rs")
            Option(rs).foreach(_.close())
            println(s"closing conn: $conn")
            Option(conn).foreach(_.close())
        }
    }

    def hiveToDF(jdbcUrl: String, sql: String, tableName: String, sps: SparkSession): DataFrame = {
        val (schema, rowList) = hiveToRows(jdbcUrl, sql, tableName)
        rowsToDF(schema, rowList, sps)
    }

    def jdbcToView(jdbcUrl: String,
                   sql: String,
                   upperBound: Long,
                   lowerBound: Long,
                   viewName: String,
                   tableName: String = ""): Boolean = {
        val df: DataFrame = jdbcToDF(jdbcUrl, sql, upperBound, lowerBound, tableName)
        df.createTempView(viewName)
        true
    }

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
    def rs(sql: String)(implicit conn: Connection): ResultSet = {
        if (StringUtils.isBlank(sql)) return null
        conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(sql)
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
}