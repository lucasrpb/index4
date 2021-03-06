package index

import scala.collection.concurrent.TrieMap

class Context(store: Storage) {

  val parents = TrieMap[B, (Option[B], Int)]()
  val blocks = TrieMap[B, Block]()

  def get(id: B): Option[Block] = {
    blocks.get(id) match {
      case None => store.get(id)
      case block => block
    }
  }

  def getLeaf(id: B): Option[Leaf] = {
    get(id).map(_.asInstanceOf[Leaf])
  }

  def getMeta(id: B): Option[Meta] = {
    get(id).map(_.asInstanceOf[Meta])
  }

}
