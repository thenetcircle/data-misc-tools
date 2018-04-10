package process

import java.lang.reflect.Modifier
import java.time.Duration
import java.util.Date

import annotation.ProcDescription
import com.google.gson.{Gson, GsonBuilder}
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.commons.lang3.{StringUtils, SystemUtils}
import sun.misc.{Signal, SignalHandler}

object MiscHelper {
    def toTimeStr(time: Long): String = DateFormatUtils.format(new Date(time), "yyyy-MM-dd hh:mm:ss")

    val GSON: Gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.TRANSIENT).create()

    def getBeanDescription(obj: Object): String = {
        if (obj == null) return ""

        val bd: ProcDescription = getProcDesc(obj)
        return Option(bd).map(_.description).getOrElse("unknown")
    }

    def getProcDesc(obj: Object): ProcDescription = {
        val clz: Class[_] = obj.getClass
        val bd: ProcDescription = clz.getAnnotation(classOf[ProcDescription])
        bd
    }

    def registSignalHandler(singalStr: String)(handler: SignalHandler): Unit = {
        if (!SystemUtils.IS_OS_LINUX) return
        Signal.handle(new Signal(singalStr), handler)
    }

    def regSignalHandler(singalStr: String)(handler: (Signal) => Unit): Unit = {
        if (!SystemUtils.IS_OS_LINUX) return
        val wrapper: SignalHandlerWrapper = new SignalHandlerWrapper(handler)
        val previousHandler: SignalHandler = Signal.handle(new Signal(singalStr), wrapper)
        wrapper.previousHandler = previousHandler
    }

    def onExit(handler: (Signal) => Unit): Unit = {
        Seq("TERM", "HUP", "INT").foreach(regSignalHandler(_)(handler))
    }

    def parseDuration(durationStr: String): Duration = Option(durationStr)
        .map(StringUtils.defaultIfBlank(_, "1M"))
        .map(Duration.parse(_))
        .getOrElse(Duration.ofMinutes(1))

    class SignalHandlerWrapper(val handler: (Signal) => Unit) extends SignalHandler {
        override def handle(signal: Signal): Unit = {
            Option(handler).foreach(_ (signal))
            Option(previousHandler).foreach(_.handle(signal))
        }

        private[process] var previousHandler: SignalHandler = _
    }
}

