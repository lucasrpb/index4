package index

trait Block {

  val id: B

  var size = 0
  var length = 0

  def isFull(): Boolean
  def isEmpty(): Boolean
  def hasMinimumSize(): Boolean
  def max: Option[B]

}
