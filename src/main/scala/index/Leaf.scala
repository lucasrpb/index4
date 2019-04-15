package index

import java.util.UUID

class Leaf(override val id: B,
           val MIN: Int,
           val MAX: Int,
           val LIMIT: Int)(implicit val comp: Ordering[B]) extends Block {

  var keys = Array.empty[Tuple]

  def size = keys.map{case (k, v) => k.length + v.length}.sum
  def length = keys.length

  def find(k: B, start: Int, end: Int): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = comp.compare(k, keys(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return find(k, start, pos - 1)

    find(k, pos + 1, end)
  }

  def insertAt(k: B, v: B, idx: Int): (Boolean, Int) = {

    keys = keys ++ Array.ofDim[Tuple](1)

    for(i<-length until idx by -1){
      keys(i) = keys(i - 1)
    }

    keys(idx) = k -> v

    //size += k.length + v.length
    //length += 1

    true -> idx
  }

  def insert(k: B, v: B): (Boolean, Int) = {
    //if(isFull()) return false -> 0

    val (found, idx) = find(k, 0, length - 1)

    if(found) return false -> 0

    insertAt(k, v, idx)
  }

  def calcMaxLen(data: Seq[Tuple], max_size: Int): (Int, Int) = {
    var i = 0
    var bytes = 0
    val len = data.length

    while(i < len){
      val (k, v) = data(i)
      bytes += k.length + v.length

      if(bytes > max_size) return bytes -> i

      i += 1
    }

    bytes -> i
  }

  def insert(data: Seq[Tuple]): (Boolean, Int) = {
    //if(isFull()) return false -> 0

    val (_, len) = calcMaxLen(data, MAX - size)

    //val len = Math.min(data.length, MAX - length)

    /*for(i<-0 until len){
      val (k, v) = data(i)
      if(!insert(k, v)._1) return false -> 0
    }*/

    assert(len > 0)

    keys = keys ++ data.slice(0, len)
    keys = keys.sortBy(_._1)

    true -> len
  }

  def split()(implicit ctx: Context): Leaf = {
    val right = new Leaf(UUID.randomUUID.toString.getBytes, MIN, MAX, LIMIT)

    ctx.blocks += right.id -> right

    var (_, len) = calcMaxLen(keys, size/2)

    //println(s"sizes ${sizes} half: $half\n")

    assert(len > 0)

    right.keys = keys.slice(len, length)
    //right.length = right.keys.length
    //right.size = right.keys.map{case (k, v) => k.length + v.length}.sum

    keys = keys.slice(0, len)
    //length = keys.length
    //size = keys.map{case (k, v) => k.length + v.length}.sum

    right
  }

  def copy()(implicit ctx: Context): Leaf = {
    if(ctx.blocks.isDefinedAt(id)) return this

    val copy = new Leaf(UUID.randomUUID.toString.getBytes, MIN, MAX, LIMIT)

    ctx.blocks += copy.id -> copy
    ctx.parents += copy.id -> ctx.parents(id)

    //copy.length = length
    //copy.size = size

    copy.keys = Array.ofDim[Tuple](length)

    for(i<-0 until length){
      copy.keys(i) = keys(i)
    }

    copy
  }

  override def max: Option[B] = {
    if(isEmpty()) return None
    Some(keys(length - 1)._1)
  }

  override def isFull(data: Seq[Tuple]): Boolean = {
    val (k, v) = data(0)
    val bytes = k.length + v.length

    bytes + size >= LIMIT

    //length == MAX
  }

  override def isEmpty(): Boolean = size == 0

  def inOrder(): Seq[Tuple] = {
    if(isEmpty()) return Seq.empty[Tuple]
    //keys.slice(0, length)
    keys
  }

  override def toString: String = inOrder().map{case (k, _) => new String(k)}.toString()
  def sizes() = inOrder().map{case (k, v) => k.length + v.length}
}
