package index

import scala.collection.concurrent.TrieMap

class MemoryStorage extends Storage {

  val blocks = TrieMap[B, Block]()

  override def get(id: B): Option[Block] = {
    blocks.get(id)
  }

  override def put(bs: TrieMap[B, Block]): Boolean = {
    bs.foreach { case (k, p) =>
      blocks.put(k, p)
    }

    true
  }

  override def put(id: B, p: Block): Boolean = {
    blocks.put(id, p)
    true
  }
}
