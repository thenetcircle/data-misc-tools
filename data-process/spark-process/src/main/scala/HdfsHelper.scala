import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs._
import org.apache.log4j.{LogManager, Logger}

/**
  * Created by fan on 2016/12/15.
  */
object HdfsHelper {
    def getLogger: Logger = LogManager.getLogger(HdfsHelper.getClass)

    lazy val _hdfs: FileSystem = FileSystem.get(InterpreterLoader.sc.hadoopConfiguration)

    def fileStatus(pathStr: String): FileStatus = {
        if (StringUtils.isBlank(pathStr)) return null
        return _hdfs.getFileStatus(new Path(pathStr))
    }

    def readFile(pathStr: String): String = {
        val log: Logger = getLogger

        if (StringUtils.isBlank(pathStr)) return StringUtils.EMPTY

        var fsis: FSDataInputStream = null
        try {
            val path: Path = new Path(pathStr)
            if (!(_hdfs.isFile(path)))
                return StringUtils.EMPTY

            fsis = _hdfs.open(path)
            fsis.setDropBehind(true)
            return IOUtils.toString(fsis)
        } catch {
            case e: Throwable => log.error(s"Exception while reading $pathStr: ", e)
        } finally {
            IOUtils.closeQuietly(fsis)
        }
        return StringUtils.EMPTY
    }
}
