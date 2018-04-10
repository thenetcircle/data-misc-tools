package process

import java.io.{Closeable, PrintWriter, StringWriter}
import java.net.{URL, URLClassLoader => JURLClassLoader}
import java.nio.file.Paths
import java.time.Duration

import annotation.ProcDescription
import javax.script.ScriptException
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.FileStatus
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Time
import records.{ExecutionRecord, RecordDao, ScriptSrc}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain

/**
  * Created by fan on 2016/12/15.
  * actually helpless
  */
object ScriptHelper {
    def log: Logger = LogManager.getLogger(ScriptHelper.getClass)

    type RDDFunction = (SparkSession, RDD[_], Time) => Any
    type TaskFunction[P] = (SparkSession, P) => Any

    def eachLine(scriptStr: String): Array[(Int, String)] = scriptStr.split('\n')
        .map(_.trim)
        .zipWithIndex.map(_.swap)

    def scriptWithLineNumber(scriptStr: String): String = eachLine(scriptStr)
        .map(line => s"${line._1}\t${line._2}")
        .mkString("\n")

    trait TExecutionValidation {
        def isReady(scriptPath: String): Boolean = {
            val procDesc: ProcDescription = MiscHelper.getProcDesc(this)
            if (procDesc == null) return true

            val interval: Duration = MiscHelper.parseDuration(procDesc.interval())
            val last: ExecutionRecord = RecordDao.getLastExecution(scriptPath)
            Option(last)
                .map(rec => interval.compareTo(Duration.ofMillis(System.currentTimeMillis() - rec.startedAt)) < 0)
                .getOrElse(true)
        }
    }

    case class ScriptTask(val fileStatus: FileStatus,
                          val task: TaskFunction[_],
                          val scriptSrc: String,
                          var scriptException: ScriptException) {
        def isCompiled: Boolean = scriptException == null
    }

    private val taskScriptCache: mutable.HashMap[String, ScriptTask] = mutable.HashMap.empty

    @throws[ScriptException]
    def getTaskInstance[P: ClassTag](scriptPath: String, sw: StringWriter = new StringWriter())
                                    (implicit dataType: Manifest[P]): (ScriptTask, ScriptTask) = {
        val currentScriptFileStatus: FileStatus = HdfsHelper.fileStatus(scriptPath)
        val cached: ScriptTask = taskScriptCache.getOrElse(scriptPath, null)
        if (cached != null && cached.fileStatus.getModificationTime == currentScriptFileStatus.getModificationTime) {
            return (null, cached)
        }

        val scriptStr: String = HdfsHelper.cat(scriptPath)
        val intp: IMain = InterpreterLoader.intp
        if (intp == null) {
            throw new IllegalStateException("InterpreterLoader.intp is not created!")
        }

        try {
            val evaluated: AnyRef = intp.eval(scriptStr)
            val func: TaskFunction[P] = evaluated.asInstanceOf[TaskFunction[P]]
            val current: ScriptTask = ScriptTask(currentScriptFileStatus, func, scriptStr, null)
            val previous: Option[ScriptTask] = taskScriptCache.put(scriptPath, current)
            return (previous.getOrElse(null), current)
        } catch {
            case e: ScriptException => {
                log.error(s"failed to compile $scriptStr", e)
                return (taskScriptCache.get(scriptPath).getOrElse(null), ScriptTask(currentScriptFileStatus, null, scriptStr, e))
            }
        }
    }

    def processTaskScript[P: ClassTag](spark: SparkSession,
                                       scriptPath: String,
                                       params: P)
                                      (implicit dataType: Manifest[P]): Any = {

        val rec: ExecutionRecord = new ExecutionRecord
        rec.startedAt = System.currentTimeMillis()
        val src: ScriptSrc = new ScriptSrc
        rec.scriptSrc = src
        src.path = scriptPath
        src.scriptType = FilenameUtils.getExtension(scriptPath)

        var _then: ScriptTask = null
        var _now: ScriptTask = null
        val sw: StringWriter = new StringWriter()

        var shouldRecord: Boolean = true

        try {
            val thenAndNow: (ScriptTask, ScriptTask) = getTaskInstance(scriptPath, sw)
            _then = thenAndNow._1
            _now = thenAndNow._2

            if (_then != null) { //if the script is modified, clean up the previous instance resource
                log.info(s"script at ${_then.fileStatus.getPath} is updated from \n\t${_then.scriptSrc} to \n\t${_now.scriptSrc}\n\tfinalize...")
                if (_then.isInstanceOf[Closeable]) _then.asInstanceOf[Closeable].close()
            }

            if (!_now.isCompiled) {
                val e: ScriptException = _now.scriptException
                val errMsg: String = s"Exception while compiling ${scriptPath: String} (line:${e.getLineNumber},\tcolumn:${e.getColumnNumber}) \n ${eachLine(_now.scriptSrc)}}: "
                log.error(errMsg, e)

                src.name = "failed to compile script"
                src.description = "failed to compile script"
                rec.result = errMsg
                rec.status = "failed"
                return
            }

            src.createdAt = _now.fileStatus.getModificationTime

            if (_now.isInstanceOf[TExecutionValidation]) {
                shouldRecord = _now.asInstanceOf[TExecutionValidation].isReady(scriptPath)
                if (!shouldRecord) return
            }

            src.name = _now.task.getClass.getName
            src.description = MiscHelper.getBeanDescription(_now.task)
            src.content = _now.scriptSrc

            val func: TaskFunction[P] = _now.task.asInstanceOf[TaskFunction[P]]
            val result: Any = func.apply(spark, params)
            rec.result = String.valueOf(result)
            rec.status = "succeeded"
        } catch {
            case e: Throwable =>
                val errMsg: String = s"Exception while executing ${scriptPath: String} \n ${_now} \n ${sw.getBuffer}: "
                rec.status = "failed"
                rec.result = errMsg
                log.error(errMsg, e)
                return e
        } finally {
            if (shouldRecord) {
                rec.finishedAt = System.currentTimeMillis()
                RecordDao.save(rec)
            }
            sw.close()
        }
    }

    def prepareScriptEngine(logWriter: PrintWriter = new PrintWriter(System.out), settings: Settings = new Settings()): IMain = {
        val cl: ClassLoader = Thread.currentThread.getContextClassLoader.getParent
        if (!cl.isInstanceOf[JURLClassLoader]) {
            throw new IllegalArgumentException("wrong classLoader")
        }
        val ucl: JURLClassLoader = cl.asInstanceOf[JURLClassLoader]
        val urls: Array[URL] = ucl.getURLs.filter(url => Paths.get(url.toURI).toFile.isFile)
        val intp: IMain = new IMain(settings, logWriter)
        intp.initializeSynchronous()
        intp.setContextClassLoader()
        return intp
    }
}
