import java.io.{Closeable, PrintWriter, StringWriter}
import java.net.{URL, URLClassLoader => JURLClassLoader}
import java.nio.file.Paths
import javax.script.ScriptException

import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.FileStatus
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Time

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.internal.util.BatchSourceFile
import scala.reflect.io.PlainFile
import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.util.AbstractFileClassLoader
import scala.tools.nsc.{Global, Settings}

/**
  * Created by fan on 2016/12/15.
  * actually helpless
  */
object ScriptHelper {
    def log: Logger = LogManager.getLogger(ScriptHelper.getClass)

    type RDDFunction = (SparkSession, RDD[_], Time) => Any
    type TaskFunction[P] = (SparkSession, P, Time) => Any

    def prepareScriptEngine(logWriter: PrintWriter = new PrintWriter(System.out), settings: Settings = new Settings()): IMain = {
        val cl: ClassLoader = Thread.currentThread.getContextClassLoader.getParent
        if (!cl.isInstanceOf[JURLClassLoader]) {
            throw new IllegalArgumentException("wrong classLoader")
        }
        val ucl: JURLClassLoader = cl.asInstanceOf[JURLClassLoader]
        val urls: Array[URL] = ucl.getURLs.filter(url => Paths.get(url.toURI).toFile.isFile)
        //        log.info(s"\n\t current class paths: \n\t ${urls.size}")

        val intp: IMain = new IMain(settings, logWriter)
        intp.initializeSynchronous()
        intp.setContextClassLoader()
        //        intp.addUrlsToClassPath(urls: _*)

        return intp
    }

    def compileAndGetInstance[R](iMain: IMain, sourcePath: String, source: String): R = {
        val compileResult = iMain.compileSourcesKeepingRun(new BatchSourceFile(new PlainFile(scala.reflect.io.Path.string2path(sourcePath)), source.toCharArray))
        val run: Global#Run = compileResult._2

        if (!compileResult._1) {
            log.error("failed to compile {} for \n {} code: \n {}", sourcePath, run, source)
            return null.asInstanceOf[R]
        }

        val resClzName: String = FilenameUtils.getBaseName(sourcePath)
        val scalaCompilerClassLoader: AbstractFileClassLoader = iMain.classLoader
        val resCls: Class[_] = scalaCompilerClassLoader.loadClass(resClzName)
        return resCls.newInstance().asInstanceOf[R]
    }

    def scriptWithLine(scriptStr: String): Array[(Int, String)] = {
        return scriptStr.split('\n').zipWithIndex.map(_.swap)
    }

    def scriptWithLineNumber(scriptStr: String): String = {
        return scriptWithLine(scriptStr).map(line => s"${line._1}\t${line._2}").mkString("\n")
    }

    private val taskScriptCache: mutable.HashMap[String, (FileStatus, TaskFunction[_], String)] = mutable.HashMap.empty

    def processTaskScript[P: ClassTag](spark: SparkSession,
                                       scriptPath: String,
                                       params: P,
                                       lastTime: Time = Time(System.currentTimeMillis()))
                                      (implicit dataType: Manifest[P]): Any = {
        val currentScriptFileStatus: FileStatus = HdfsHelper.fileStatus(scriptPath)
        if (!currentScriptFileStatus.isFile) {
            val errMsg: String = s"${scriptPath} is not valid file"
            log.error(errMsg)
            throw new IllegalArgumentException(errMsg)
        }

        val cached: (FileStatus, TaskFunction[_], String) = taskScriptCache.getOrElse(scriptPath, null)
        if (cached != null && cached._1.getModificationTime == currentScriptFileStatus.getModificationTime) {
            log.info(s"\n\tprocessing $dataType with script: $scriptPath in cache\n")
            try {
                val func: TaskFunction[P] = cached._2.asInstanceOf[TaskFunction[P]]
                val result: Any = func.apply(spark, params, lastTime)
                log.info(s"result:\t $result")
                return result
            } catch {
                case e: Throwable =>
                    log.error(s"Exception while executing ${scriptPath: String} \n ${cached._3} \n", e)
                    return e
            }
        }

        log.info(s"\n\tprocessing RDD[$dataType] with script: $scriptPath modified at ${currentScriptFileStatus.getModificationTime}\n")

        val scriptStr: String = HdfsHelper.readFile(scriptPath)
        if (StringUtils.isBlank(scriptStr)) {
            log.error(s"$scriptPath: String is invalid")
            return null
        }

        val sw: StringWriter = new StringWriter()
        val intp: IMain = if (InterpreterLoader.intp != null) {
            InterpreterLoader.intp
        } else {
            throw new IllegalStateException("InterpreterLoader.intp is not created!")
        }

        try {
            val evaluated: AnyRef = intp.eval(scriptStr)
            if (!evaluated.isInstanceOf[TaskFunction[P]]) {
                log.error(s"$scriptPath: String is invalid \n ${sw.getBuffer} \n $scriptStr")
                return null
            }

            intp.bind("log", log)
            val func: TaskFunction[P] = evaluated.asInstanceOf[TaskFunction[P]]
            val result: Any = func.apply(spark, params, lastTime)

            val previous: Option[(FileStatus, TaskFunction[_], String)] = taskScriptCache.put(scriptPath, (currentScriptFileStatus, func, scriptStr))
            if (previous.nonEmpty) {
                val last: (FileStatus, TaskFunction[_], String) = previous.get
                log.info(s"script at ${last._3} is updated from \n\t${last._1} to \n\t$currentScriptFileStatus\n\tfinalize...")
                if (last.isInstanceOf[Closeable]) {
                    last.asInstanceOf[Closeable].close()
                }
            }

            log.info(s"result:\t $result")
            return result
        } catch {
            case e: ScriptException =>
                log.error(s"Exception while compiling ${scriptPath: String} (line:${e.getLineNumber},\tcolumn:${e.getColumnNumber}) \n ${scriptWithLine(scriptStr)} \n ${sw.getBuffer}: ", e)
                return null
            case e: Throwable =>
                log.error(s"Exception while executing ${scriptPath: String} \n ${scriptWithLine(scriptStr)} \n ${sw.getBuffer}: ", e)
                return e
        } finally {
            sw.close()
        }
    }

    def classLoaderTree(clzLoader: ClassLoader): String = {
        import scala.collection.mutable.ListBuffer
        val cldList: ListBuffer[ClassLoader] = ListBuffer(clzLoader)
        while (cldList.last.getParent != null || cldList.size > 100) {
            cldList += cldList.last.getParent
        }
        cldList.mkString("\n\t")
    }
}