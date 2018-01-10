package entity

import java.util.{Date, Objects}

import scala.beans.BeanProperty

trait TEntity extends Serializable {
    @BeanProperty val kind: String = this.getClass.getCanonicalName
    @BeanProperty var name: String = _
    @BeanProperty var id: String = _
    @BeanProperty var createdAt: Date = _
    @BeanProperty var version: Int = _

    override def toString = s"$kind(name=$name, id=$id, createdAt=$createdAt, version=$version)"

    override def hashCode(): Int = Objects.hash(kind, id, name)


    def canEqual(other: Any): Boolean = other.isInstanceOf[TEntity]

    override def equals(other: Any): Boolean = other match {
        case that: TEntity =>
            (that canEqual this) &&
                kind == that.kind &&
                id == that.id &&
                name == that.name &&
                version == that.version
        case _ => false
    }
}
