package process

import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

/**
  * Created by fan on 2016/12/15.
  */
object HdfsHelper {
    val sc: SparkContext = InterpreterLoader.sc

    val log: Logger = LogManager.getLogger(HdfsHelper.getClass)
    lazy val _hdfs: FileSystem = FileSystem.get(sc.hadoopConfiguration)

    def fileStatus(pathStr: String): FileStatus = {
        if (StringUtils.isBlank(pathStr)) return null
        return _hdfs.getFileStatus(new Path(pathStr))
    }

    def cat(pathStr: String): String = {
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

    def append(pathStr: String, str: String): String = {
        if (StringUtils.isBlank(pathStr)) return StringUtils.EMPTY
        var fsos: FSDataOutputStream = null

        try {
            val path: Path = new Path(pathStr)
            fsos = if (!(_hdfs.isFile(path))) {
                _hdfs.create(path)
            } else {
                _hdfs.append(path)
            }

            fsos.write(str.getBytes)
            fsos.hflush()
        } catch {
            case e: Throwable => log.error(s"Exception while appending \n$str \nto \n$pathStr: ", e)
        } finally {
            IOUtils.closeQuietly(fsos)
        }
        return str
    }

    def ls(pathStr: String, recursive: Boolean = false): List[Path] = {
        if (StringUtils.isBlank(pathStr)) return List.empty

        val path: Path = new Path(pathStr)
        if (!_hdfs.exists(path)) return List.empty

        val it: RemoteIterator[LocatedFileStatus] = _hdfs.listFiles(path, recursive)
        val buf: ListBuffer[LocatedFileStatus] = ListBuffer.empty
        while (it.hasNext) buf += it.next()
        buf.map(_.getPath).toList
    }

}
