package process_line

import entity.TEntity

import scala.beans.BeanProperty

trait TDataProc extends TEntity {
    @BeanProperty val input: Iterable[TEntity] = _
    @BeanProperty val output: Iterable[TEntity] = _


}
