package index

import java.util.UUID

class Meta(override val id: B,
           val MIN: Int,
           val MAX: Int,
           val LIMIT: Int)(implicit val comp: Ordering[B]) extends Block {

  var pointers = Array.empty[Tuple]

  def find(k: B, start: Int, end: Int): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = comp.compare(k, pointers(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return find(k, start, pos - 1)

    find(k, pos + 1, end)
  }

  def findPath(k: B): Option[B] = {
    if (isEmpty()) return None
    val (_, pos) = find(k, 0, length - 1)

    Some(pointers(if(pos < length) pos else pos - 1)._2)
  }

  def setChild(k: B, c: B, idx: Int)(implicit ctx: Context): Unit = {
    pointers(idx) = k -> c
    ctx.parents += c -> (Some(id), idx)
  }

  def setPointers()(implicit ctx: Context): Unit = {
    for(i<-0 until length){
      val (_, c) = pointers(i)
      ctx.parents += c -> (Some(id), i)
    }
  }

  def insertAt(k: B, v: B, idx: Int): (Boolean, Int) = {
    for(i<-length until idx by -1){
      pointers(i) = pointers(i - 1)
    }

    pointers(idx) = k -> v

    size += k.length + v.length
    length += 1

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

  def insert(data: Seq[Tuple])(implicit ctx: Context): (Boolean, Int) = {
    //if(isFull()) return false -> 0

    val (_, len) = calcMaxLen(data, MAX - size)

    pointers = pointers ++ Array.ofDim[Tuple](len)

    for(i<-0 until len){
      val (k, v) = data(i)
      if(!insert(k, v)._1) return false -> 0
    }

    setPointers()

    true -> len
  }

  def split()(implicit ctx: Context): Meta = {
    val right = new Meta(UUID.randomUUID.toString.getBytes, MIN, MAX, LIMIT)

    ctx.blocks += right.id -> right

    val half = size/2
    //var (_, len) = calcMaxLen(pointers, half)

    val len = length/2

    assert(len > 0)

    right.pointers = pointers.slice(len, length)
    right.length = right.pointers.length
    right.size = right.pointers.map{case (k, v) => k.length + v.length}.sum

    right.setPointers()

    pointers = pointers.slice(0, len)
    length = pointers.length
    size = pointers.map{case (k, v) => k.length + v.length}.sum

    setPointers()

    right
  }

  def copy()(implicit ctx: Context): Meta = {

    if(ctx.blocks.isDefinedAt(id)) return this

    val copy = new Meta(UUID.randomUUID.toString.getBytes, MIN, MAX, LIMIT)

    ctx.blocks += copy.id -> copy
    ctx.parents += copy.id -> ctx.parents(id)

    copy.length = length
    copy.size = size

    copy.pointers = Array.ofDim[Tuple](length)

    for(i<-0 until length){
      copy.pointers(i) = pointers(i)
    }

    copy.setPointers()

    copy
  }

  override def max: Option[B] = {
    if(isEmpty()) return None
    Some(pointers(length - 1)._1)
  }

  override def isFull(data: Seq[Tuple]): Boolean = {
    val (k, v) = data(0)
    val bytes = k.length + v.length

    bytes + size >= LIMIT
  }

  override def isEmpty(): Boolean = size == 0

  def inOrder(): Seq[Tuple] = {
    if(isEmpty()) return Seq.empty[Tuple]
    pointers.slice(0, length)
  }
}
