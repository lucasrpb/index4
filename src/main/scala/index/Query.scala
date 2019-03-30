package index

object Query {

  def inOrder(start: Option[B])(implicit store: Storage): Seq[Tuple] = {
    start match {
      case None => Seq.empty[Tuple]
      case Some(id) => store.get(id).get match {
        case leaf: Leaf => leaf.inOrder()
        case meta: Meta =>
          meta.pointers.slice(0, meta.length).foldLeft(Seq.empty[Tuple]) { case (b, (_, n)) =>
            b ++ inOrder(Some(n))
          }
      }
    }
  }

}
