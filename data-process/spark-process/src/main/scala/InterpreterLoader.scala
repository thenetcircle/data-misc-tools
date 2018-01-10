import java.io.File
import java.util.Date
import java.util.concurrent.TimeUnit

import Configs.SCRIPT_PATH
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Time
import org.apache.spark.{SparkConf, SparkContext}

import scala.tools.nsc.interpreter.Results.Result
import scala.tools.nsc.interpreter.{ILoop, IMain, Results}
import scala.tools.nsc.{GenericRunnerSettings, Settings}

object InterpreterLoader {
    def log: Logger = LogManager.getLogger(InterpreterLoader.getClass)

    var lastResult: Any = _
    var lastInvocationTime: Long = System.currentTimeMillis()

    /**
      * In YARN mode this method returns a union of the jar files pointed by "spark.jars" and the
      * "spark.yarn.dist.jars" properties, while in other modes it returns the jar files pointed by
      * only the "spark.jars" property.
      * noted, we only run this with yarn
      */
    def getUserJars(conf: SparkConf): Seq[String] = {
        val sparkJars: Option[String] = conf.getOption("spark.jars")
        val yarnJars: Option[String] = conf.getOption("spark.yarn.dist.jars")
        val sparkJarList: List[String] = sparkJars.map(_.split(",")).getOrElse(Array.empty).toList
        val yarnJarList: List[String] = yarnJars.map(_.split(",")).getOrElse(Array.empty).toList
        return (sparkJarList ::: yarnJarList).filter(_.nonEmpty).toSet.toList
    }

    var sc: SparkContext = _
    var spark: SparkSession = _
    val conf: SparkConf = Configs.SPARK_CONF
    val outputDir: File = {
        val outputDirPath: String = FileUtils.getTempDirectory.getAbsolutePath + "/spark-process/" + DateFormatUtils.format(new Date(), "yyyy-MM-dd_hh-mm-ss")
        log.info(s"going to create outputDir:\n\t$outputDirPath")
        val outputDir: File = FileUtils.getFile(outputDirPath)
        if (outputDir.mkdir()) {
            outputDir
        } else
            null
    }

    var sparkLoop: ILoop = _
    var intp: IMain = _

    private var hasError: Boolean = false

    private def scalaOptionError(msg: String): Unit = {
        hasError = true
        Console.err.println(msg)
    }

    def prepareSettings(args: Array[String], conf: SparkConf): Settings = {
        if (outputDir == null || !outputDir.exists()) {
            val errMsg: String = s"failed to create output dir: ${outputDir}"
            log.error(errMsg)
            throw new IllegalStateException(errMsg)
        }

        val classPath: String = getUserJars(conf).mkString(File.pathSeparator)
        log.info(s"running with -classpath:\n\t$classPath")

        val intpArgs: List[String] = List(
            "-Yrepl-class-based",
            "-Yrepl-outdir", s"${outputDir.getAbsolutePath}",
            "-classpath", classPath
        ) ++ args.toList

        val settings: GenericRunnerSettings = new GenericRunnerSettings(scalaOptionError)
        settings.usejavacp.value = true
        settings.embeddedDefaults(SparkSubmit.getClass.getClassLoader)
        settings.classpath.value = sys.props("java.class.path")
        val (result: Boolean, argList: List[String]) = settings.processArguments(intpArgs, true)
        if (!result) {
            val errMsg: String = s"failed to process args:\n\t${intpArgs.mkString("\n\t")}"
            log.error(errMsg)
            throw new IllegalArgumentException(errMsg)
        }
        return settings
    }

    def bindAndCheck(key: String, value: Any): Unit = {
        val bindResult: Result = intp.bind(key, value)
        bindResult match {
            case Results.Success =>
                log.info(s"binding success:\n\t$key to \n\t ${intp.eval(key)}\n")
            case _ =>
                log.info(s"binding otherwise:\n\t$key\n\t$bindResult\n")
                throw new Exception(s"binding $key $bindResult\n")
        }
    }

    def main(args: Array[String]): Unit = {
        //Noted!! "spark.repl.class.outputDir" can not be missed, otherwise, spark can't distribute class to executors
        Configs.SPARK_CONF.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)
        intp = ScriptHelper.prepareScriptEngine(settings = prepareSettings(args, Configs.SPARK_CONF)) //sparkLoop.intp
        log.info(intp)
        try {
            //            bindAndCheck("conf", prepareSparkConf())
            //            bindAndCheck("ScriptHelper", ScriptHelper)
            //            bindAndCheck("KafkaHelper", KafkaHelper)
            //            bindAndCheck("HdfsHelper", HdfsHelper)
            //            bindAndCheck("InterpreterLoader", InterpreterLoader)

            //todo refer to org.apache.spark.repl.SparkILoop line 39
            spark = intp.eval(
                """
                  |try {
                  |org.apache.spark.sql.SparkSession.builder.config(Configs.SPARK_CONF).getOrCreate()
                  |} catch {
                  |case e: Throwable => e.printStackTrace()
                  |}
                """.stripMargin).asInstanceOf[SparkSession]

            bindAndCheck("spark", spark)
            sc = spark.sparkContext
            log.info(s"current spark configurations:\n\t${sc.getConf.getAll.sorted.mkString("\n\t")}\n")
            bindAndCheck("sc", sc)

            val oneMinInMS: Long = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES)
            while (!lastResult.isInstanceOf[Throwable]) {
                val preStartTime: Long = System.currentTimeMillis()
                execTask()
                Thread.sleep(Math.max(oneMinInMS - (System.currentTimeMillis() - preStartTime), 0))
            }
        } catch {
            case e: Throwable => log.error(s"failed to eval script at: ${args}", e)
        } finally {
            Option(spark).foreach(_.stop())
            Option(intp).foreach(_.close())
        }
    }

    @throws[Exception]
    def execTask(): Unit = {
        try {
            lastResult = ScriptHelper.processTaskScript[Any](spark, SCRIPT_PATH, lastResult, Time(lastInvocationTime))
        } catch {
            case e: Throwable =>
                log.error("failed to process Script", e)
                lastResult = e
                throw e
        } finally {
            lastInvocationTime = System.currentTimeMillis()
        }
    }
}
