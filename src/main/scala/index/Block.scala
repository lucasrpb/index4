package index

trait Block {

  val id: B

  //var size = 0
  //var length = 0

  def max: Option[B]
  def isFull(data: Seq[Tuple]): Boolean
  def isEmpty(): Boolean
  def inOrder(): Seq[Tuple]

}
