package object index {

  type B = Array[Byte]
  type Tuple = (B, B)
  type Pointer = (B, B)

  case class Ref(root: Option[B] = None, size: Int = 0)

}
