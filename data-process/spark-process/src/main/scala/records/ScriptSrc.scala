package records

import javax.jdo.annotations._
import process.MiscHelper._

import scala.beans.BeanProperty

@EmbeddedOnly
@PersistenceCapable
@PersistenceAware
class ScriptSrc extends Serializable {
    @BeanProperty var createdAt: Long = _
    @BeanProperty var name: String = _
    @BeanProperty var modifiedAt: Long = _

    @Index
    @Column(length = 500)
    @BeanProperty var path: String = _

    @BeanProperty var scriptType: String = _
    @BeanProperty var description: String = _

    @Column(jdbcType = "CLOB")
    @BeanProperty var content: String = _

    def canEqual(other: Any): Boolean = other.isInstanceOf[ScriptSrc]

    override def toString =
        s"""
           |{clz: 'ScriptSrc',
           |    name: '$name',
           |    path: '$path',
           |    scriptType: '$scriptType',
           |    description: '$description',
           |    content: '$content',
           |    createdAt: '${toTimeStr(createdAt)}',
           |    modifiedAt: '${toTimeStr(modifiedAt)}'}
         """.stripMargin

    override def equals(other: Any): Boolean = other match {
        case that: ScriptSrc =>
            (that canEqual this) &&
                createdAt == that.createdAt &&
                path == that.path
        case _ => false
    }

    override def hashCode(): Int = {
        val state = Seq(createdAt, path)
        state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
}
