package index

import scala.collection.concurrent.TrieMap

class MemoryStorage extends Storage {

  val blocks = TrieMap[B, Block]()

  override def get(id: B): Option[Block] = {
    blocks.get(id)
  }

  override def put(blocks: TrieMap[B, Block]): Boolean = {
    blocks.foreach { case (k, v) =>
      this.blocks += k -> v
    }

    true
  }
}
