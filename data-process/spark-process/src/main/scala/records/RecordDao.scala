package records

import java.util

import javax.jdo._
import org.apache.log4j.{LogManager, Logger}
import org.datanucleus.store.query.QueryResult
import process.Configs._
import process.MiscHelper
import sun.misc.Signal

object RecordDao {
    def log: Logger = LogManager.getLogger(RecordDao.getClass)

    val RECORD_PATH: String = "/logs/spark-records"

    private val spark_proc: String = "spark_proc"

    def save(rec: ExecutionRecord): Unit = {
        stupidTemplate((pm: PersistenceManager) => pm.makePersistent(rec))
        log.info(s"saved result....\n\t$rec")
    }

    //TODO, apply threadlocal for multi-thread (or is it necessary for now?)
    def getPMF: PersistenceManagerFactory = {

        val props: util.HashMap[String, String] = new util.HashMap
        props.put("javax.jdo.PersistenceManagerFactoryClass", "org.datanucleus.api.jdo.JDOPersistenceManagerFactory")
        //        props.put("javax.jdo.option.PersistenceUnitName", spark_proc)
        props.put("datanucleus.ConnectionDriverName", classOf[com.mysql.jdbc.Driver].getName)
        props.put("datanucleus.ConnectionURL", RECORDS_JDBC_URL)
        props.put("datanucleus.ConnectionUserName", USERNAME)
        props.put("datanucleus.ConnectionPassword", PASSWORD)
        //        props.put("datanucleus.autoStartMechanism", "None")
        props.put("datanucleus.schema.autoCreateAll", "true")
        props.put("datanucleus.schema.autoCreateTables", "true")
        props.put("datanucleus.schema.autoCreateColumns", "true")
        props.put("datanucleus.rdbms.stringDefaultLength", "4000")
        props.put("datanucleus.schema.validateAll", "false")
        props.put("persistence-unit", spark_proc)

        val pmf: PersistenceManagerFactory = JDOHelper.getPersistenceManagerFactory(props)

        val enhancer: JDOEnhancer = JDOHelper.getEnhancer
        enhancer.addClasses(classOf[ExecutionRecord].getName, classOf[ScriptSrc].getName)
        enhancer.addPersistenceUnit(pmf.getPersistenceUnitName)
        enhancer.enhance()

        pmf
    }

    private lazy val pmf: PersistenceManagerFactory = getPMF
    private lazy val pm: PersistenceManager = pmf.getPersistenceManager

    def cleanUp(): Unit = {
        log.info("closing up")
        Option(pm).foreach((_: PersistenceManager).close())
        Option(pmf).foreach((_: PersistenceManagerFactory).close())
        log.info("closed")
    }

    {
        MiscHelper.onExit((sig: Signal) => cleanUp())
    }

    def stupidTemplate[R](op: PersistenceManager => R): R = {
        val tx: Transaction = pm.currentTransaction()
        if (!tx.isActive) {
            tx.begin()
        }

        var r: R = null.asInstanceOf[R]
        try {
            r = op(pm)
            tx.commit()
        } catch {
            case e: Throwable =>
                if (tx.isActive) tx.rollback()
                pm.evictAll()
                log.error("failed to oper", e)
        }
        return r
    }

    def getLastExecution(path: String): ExecutionRecord = {
        val q: Query[_] = pm.newQuery("JPQL", s"select er from records.ExecutionRecord er where er.scriptSrc.path='$path' order by er.startedAt desc limit 1")
        q.setResultClass(classOf[ExecutionRecord])
        val qr: QueryResult[ExecutionRecord] = q.execute().asInstanceOf[QueryResult[ExecutionRecord]]
        return qr.stream().findFirst().orElse(null)
    }

}
