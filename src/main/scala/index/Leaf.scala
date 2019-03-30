package index

import java.util.UUID

class Leaf(val id: B,
           val MIN: Int,
           val MAX: Int,
           val LIMIT: Int)(implicit val ord: Ordering[B]) extends Block {

  var keys = Array.empty[Tuple]

  def find(k: B, start: Int, end: Int): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, keys(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return find(k, start, pos - 1)

    find(k, pos + 1, end)
  }

  def insertAt(k: B, v: B, idx: Int): (Boolean, Int) = {
    for(i<-length until idx by -1){
      keys(i) = keys(i - 1)
    }

    keys(idx) = k -> v

    size += k.length + v.length
    length += 1

    true -> idx
  }

  def insert(k: B, v: B): (Boolean, Int) = {
    if(isFull()) {
      return false -> 0
    }

    val (found, idx) = find(k, 0, length - 1)

    if(found) return false -> 0

    insertAt(k, v, idx)
  }

  def calcMaxLen(data: Seq[Tuple], max: Int): (Int, Int) = {
    var i = 0
    val len = data.length
    var bytes = 0

    while(i < len){
      val (k, v) = data(i)
      val size = k.length + v.length

      if(bytes + size > max) return bytes -> i

      i += 1
      bytes += size
    }

    bytes -> i
  }

  def insert(data: Seq[Tuple]): (Boolean, Int) = {
    if(isFull()) {
      return false -> 0
    }

    //val (_, len) = calcMaxLen(data, MAX - size)

    val len = Math.min(data.length, MAX - length)

    if(len == 0) return false -> 0

    keys = keys ++ Array.ofDim[Tuple](len)

    for(i<-0 until len){
      val (k, _) = data(i)

      if(find(k, 0, length - 1)._1) return false -> 0
    }

    for(i<-0 until len){
      val (k, v) = data(i)
      if(!insert(k, v)._1) return false -> 0
    }

    true -> len
  }

  def copy()(implicit ctx: Context): Leaf = {
    if(ctx.blocks.isDefinedAt(id)) return this

    val copy = new Leaf(UUID.randomUUID.toString.getBytes(), MIN, MAX, LIMIT)

    ctx.blocks += copy.id -> copy
    ctx.parents += copy.id -> ctx.parents(id)

    copy.keys = Array.ofDim[Tuple](length)
    copy.length = length
    copy.size = size

    for(i<-0 until length){
      copy.keys(i) = keys(i)
    }

    copy
  }

  def split()(implicit ctx: Context): Leaf = {
    val right = new Leaf(UUID.randomUUID.toString.getBytes(), MIN, MAX, LIMIT)

    ctx.blocks += right.id -> right

    //val half = size/2
    //val (bytes, len) = calcMaxLen(keys, half)

    val len = length/2

    right.keys = keys.slice(len, length)
    right.length = right.keys.length
    right.size = right.keys.map{case (k, v) => k.length + v.length}.sum

    keys = keys.slice(0, len)
    length = keys.length
    size = keys.map{case (k, v) => k.length + v.length}.sum

    right
  }

  override def max: Option[B] = Some(keys(length - 1)._1)

  override def isFull(): Boolean = length >= MAX
  override def isEmpty(): Boolean = length == 0
  override def hasMinimumSize(): Boolean = length >= MIN

  def inOrder(): Seq[Tuple] = {
    if(isEmpty()) return Seq.empty[Tuple]
    keys.slice(0, length)
  }

}
