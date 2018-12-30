package records

import javax.jdo.annotations._
import process.MiscHelper._

import scala.beans.BeanProperty

@PersistenceCapable(table = "exec_rec")
@PersistenceAware
class ExecutionRecord extends Serializable {
    @Embedded
    @BeanProperty var scriptSrc: ScriptSrc = _

    @PrimaryKey
    @BeanProperty var startedAt: Long = _

    @BeanProperty var finishedAt: Long = _

    @Column(jdbcType = "CLOB")
    @BeanProperty var result: String = _
    @BeanProperty var status: String = _

    def canEqual(other: Any): Boolean = other.isInstanceOf[ExecutionRecord]

    override def equals(other: Any): Boolean = other match {
        case that: ExecutionRecord =>
            (that canEqual this) &&
                scriptSrc == that.scriptSrc &&
                startedAt == that.startedAt
        case _ => false
    }

    override def hashCode(): Int = {
        val state: Seq[Any] = Seq(scriptSrc, startedAt)
        state.map((_: Any).hashCode()).foldLeft(0)((a: Int, b: Int) => 31 * a + b)
    }

    override def toString =
        s"""ExecutionRecord(scriptSrc=$scriptSrc,
           |startedAt=${toTimeStr(startedAt)},
           |finishedAt=${toTimeStr(finishedAt)},
           |result=$result,
           |status=$status,
           |hashCode=$hashCode())""".stripMargin
}
