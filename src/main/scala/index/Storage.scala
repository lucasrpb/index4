package index

import scala.collection.concurrent.TrieMap

trait Storage {

  def get(id: B): Option[Block]
  def put(blocks: TrieMap[B, Block]): Boolean

}
