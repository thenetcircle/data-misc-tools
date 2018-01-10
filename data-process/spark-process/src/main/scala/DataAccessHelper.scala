import java.sql.{Connection, ResultSet, Statement}

import InterpreterLoader._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.{JDBC_TABLE_NAME, JDBC_URL}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object DataAccessHelper extends Serializable {

    def jdbcConnectionFactory(jdbcUrl: String): () => Connection = {
        val jdbcOpt: JDBCOptions = new JDBCOptions(Map(JDBC_URL -> jdbcUrl, JDBC_TABLE_NAME -> ""))
        JdbcUtils.createConnectionFactory(jdbcOpt)
    }

    def jdbcConnection(jdbcUrl: String): Connection = jdbcConnectionFactory(jdbcUrl)()

    def execSql(jdbcUrl: String, sql: String): Int = {
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
}