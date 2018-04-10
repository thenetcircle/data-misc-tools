package hive

import java.io._
import java.time.Duration
import java.util.Properties

import org.apache.commons.io.{FilenameUtils, IOUtils}
import org.apache.commons.lang3._
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hive.beeline.BeeLine
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import process.HdfsHelper
import process.HdfsHelper._
import process.ScriptHelper.TExecutionValidation
import records.{ExecutionRecord, RecordDao, ScriptSrc}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object HiveBeeLine {
    val MAX_COL_WIDTH: Int = 50
    val MAX_ROW_COUNT: Int = 100

    private def props: Properties = {
        val props: Properties = new Properties
        props
    }

    val _outputStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    var bl: BeeLine = _

    def init(): Unit = {
        bl = new BeeLine(false)
        bl.setErrorStream(new PrintStream(_outputStream))
        bl.setOutputStream(new PrintStream(_outputStream))
        bl.begin(Array(HiveHelper.HIVE_JDBC_URL_UDF), new ByteArrayInputStream(Array[Byte]()))
    }

    def isHelpRequest(line: String): Boolean = line.equals("?") || line.equalsIgnoreCase("help")

    def isComment(line: String): Boolean = StringUtils.startsWithAny(line.trim, "#", "--")

    def needsContinuation(line: String): Boolean = {
        if (isHelpRequest(line)) return false
        if (line.startsWith(BeeLine.COMMAND_PREFIX)) return false
        if (isComment(line)) return false

        val trimmed: String = line.trim
        if (trimmed.isEmpty) return false
        //        if (!bl.getOpts.isAllowMultiLineCommand) return false
        return !trimmed.endsWith(";")
    }

    def processSqlLines(sqlLines: String): List[String] = {
        val cmds: ListBuffer[String] = ListBuffer.empty
        val reader: BufferedReader = new BufferedReader(new StringReader(sqlLines))

        var cmd: String = null
        breakable {
            while (true) {
                var sqlLine: String = reader.readLine()
                if (sqlLine == null) break

                val trimmedLine: String = sqlLine.trim
                //                if (bl.getOpts.getTrimScripts) {
                if (true) {
                    sqlLine = trimmedLine
                }
                if (cmd != null) {
                    cmd += "\n"
                    cmd += sqlLine
                    if (trimmedLine.endsWith(";")) {
                        cmds += cmd
                        cmd = null
                    }
                } else {
                    //we re starting a new command
                    if (needsContinuation(sqlLine)) cmd = sqlLine
                    else cmds += sqlLine
                }

            }
        }
        if (cmd != null) {
            cmd += ";"
            cmds += cmd
        }
        reader.close()
        cmds.toList
    }

    def runSqlFile(sqlPathStr: String): (Array[String], Int, String) = {
        var errMsg: String = ""
        if (StringUtils.isBlank(sqlPathStr)) {
            errMsg = s"empty path: ${sqlPathStr}"
            throw new IllegalArgumentException(errMsg)
        }

        val sqlPath: Path = new Path(sqlPathStr)
        if (!(_hdfs.exists(sqlPath) && _hdfs.isFile(sqlPath))) {
            errMsg = s"not accessible file: ${sqlPath}"
            throw new IllegalArgumentException(errMsg)
        }

        val sqlStr: String = HdfsHelper.cat(sqlPathStr).trim
        if (StringUtils.isBlank(sqlStr)) {
            errMsg = s"sql is blank at ${sqlPath}\n[${sqlStr}]"
            log.error(errMsg)
            throw new IllegalArgumentException(errMsg)
        }

        val sqlLines: Array[String] = processSqlLines(sqlStr).toArray
        log.info(s"running sql: $sqlPathStr \n${sqlStr}\n\n")
        val successCount: Int = bl.runCommands(sqlLines)
        val logStr: String = new String(_outputStream.toByteArray)

        _outputStream.reset()

        (sqlLines, successCount, logStr)
    }

    def runSqlFileWithLog(sqlPathStr: String): ExecutionRecord = {
        val er: ExecutionRecord = new ExecutionRecord
        val src: ScriptSrc = new ScriptSrc

        er.scriptSrc = src

        val sqlFileStatus: FileStatus = HdfsHelper.fileStatus(sqlPathStr)
        src.createdAt = sqlFileStatus.getModificationTime
        src.name = FilenameUtils.getName(sqlPathStr)
        src.description = "hive sql"
        src.path = sqlPathStr
        src.scriptType = "hive"

        try {
            er.startedAt = System.currentTimeMillis()
            val re: (Array[String], Int, String) = runSqlFile(sqlPathStr)

            val _sqlLines: Array[String] = re._1
            val successCnt: Int = re._2
            val logStr: String = re._3

            er.status = if (_sqlLines.size == successCnt) "successful" else "fail"
            val sqlLines: String = (0 until _sqlLines.length).map(i => s"${i + 1}\t${_sqlLines(i)}").mkString("\n")
            src.content = sqlLines

            er.result =
                s"""${er.status} at sql $successCnt th
                   |
                   |$sqlLines
                   |
                   |$logStr""".stripMargin
        } catch {
            case e: Throwable => {
                er.status = "failed"
                er.result = e.getMessage
                log.error(s"failed to run sql: $sqlPathStr", e)
            }
        } finally {
            er.finishedAt = System.currentTimeMillis()
            RecordDao.save(er)
        }
        er
    }

    def runSqlWithLog(sql: String, name: String, path: String): ExecutionRecord = {
        val er: ExecutionRecord = new ExecutionRecord
        val src: ScriptSrc = new ScriptSrc

        er.scriptSrc = src

        src.createdAt = System.currentTimeMillis()
        src.name = name
        src.description = "hive sql"
        src.path = path
        src.scriptType = "hive"

        try {
            er.startedAt = System.currentTimeMillis()
            src.content = sql

            val sqlLines: Array[String] = processSqlLines(sql).toArray
            val successCount: Int = bl.runCommands(sqlLines)

            er.result = s"failed at sql ${successCount}th\n\n$sqlLines"
        } catch {
            case e: Throwable => {
                er.status = "failed"
                er.result = e.getMessage
            }
        } finally {
            er.finishedAt = System.currentTimeMillis()
            RecordDao.save(er)
        }
        er
    }

    def clean(): Unit = {
        log.info("Closing BeeLine......")
        Option(bl).foreach(_.close())
        HiveBeeLine.bl = null
        IOUtils.closeQuietly(_outputStream)
        log.info("BeeLine closed")
    }
}

import hive.HiveBeeLine._

class HiveBeeLine(val isFile: Boolean = true, val interval: String = "PT1M")
    extends ((SparkSession, String) => Any)
        with Closeable
        with TExecutionValidation {

    val log: Logger = LogManager.getLogger("HiveBeeLine.scala")

    override def isReady(scriptPath: String): Boolean = {
        val interval: Duration = Duration.parse(this.interval)
        val last: ExecutionRecord = RecordDao.getLastExecution(scriptPath)
        Option(last)
            .map(rec => interval.compareTo(Duration.ofMillis(System.currentTimeMillis() - rec.startedAt)) < 0)
            .getOrElse(true)
    }

    override def apply(sparkSession: SparkSession, params: String): Any = {
        var errMsg: String = ""
        try {
            if (!params.isInstanceOf[String]) {
                errMsg = s"invalid params: ${params}"
                return errMsg
            }
            val sc: SparkContext = sparkSession.sparkContext
            if (HiveBeeLine.bl == null) {
                log.info("HiveBeeLine.init")
                HiveBeeLine.init
            }

            if (isFile) {
                val sqlPathStr: String = params.asInstanceOf[String]
                if (!isReady(sqlPathStr))
                    return

                return runSqlFileWithLog(sqlPathStr)
            } else
                return runSqlWithLog(params, "hive", "hive")
        } finally {
            log.error(errMsg)
        }
    }

    override def close(): Unit = clean()
}

//new HiveBeeLine