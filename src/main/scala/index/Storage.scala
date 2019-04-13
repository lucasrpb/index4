package index

import scala.collection.concurrent.TrieMap

trait Storage {
  def get(id: B): Option[Block]
  def put(id: B, p: Block): Boolean
  def put(bs: TrieMap[B, Block]): Boolean
}
