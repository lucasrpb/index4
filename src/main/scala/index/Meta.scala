package index

import java.util.UUID

class Meta(val id: B,
           val MIN: Int,
           val MAX: Int,
           val LIMIT: Int)(implicit val ord: Ordering[B]) extends Block {

  var pointers = Array.empty[Pointer]

  def find(k: B, start: Int, end: Int): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, pointers(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return find(k, start, pos - 1)

    find(k, pos + 1, end)
  }

  def findPath(k: B): Option[B] = {
    if(isEmpty()) return None
    val (_, pos) = find(k, 0, length - 1)

    Some(pointers(if(pos < length) pos else pos - 1)._2)
  }

  def setChild(k: B, c: B, pos: Int)(implicit ctx: Context): Boolean = {
    pointers(pos) = k -> c
    ctx.parents += c -> (Some(id), pos)
    true
  }

  def insertAt(k: B, c: B, idx: Int)(implicit ctx: Context): (Boolean, Int) = {
    for(i<-length until idx by -1){
      val (k, c) = pointers(i - 1)
      setChild(k, c, i)
    }

    pointers(idx) = k -> c

    setChild(k, c, idx)

    size += k.length + c.length
    length += 1

    true -> idx
  }

  def insert(k: B, v: B)(implicit ctx: Context): (Boolean, Int) = {
    if(k.length + v.length > MAX) {
      return false -> 0
    }

    val (found, idx) = find(k, 0, length - 1)

    if(found) return false -> 0

    insertAt(k, v, idx)
  }

  def calcMaxLen(data: Seq[Pointer], max: Int): (Int, Int) = {
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

  def insert(data: Seq[Pointer])(implicit ctx: Context): (Boolean, Int) = {
    if(isFull()) {
      return false -> 0
    }

    val (_, len) = calcMaxLen(data, MAX - size)

    if(len == 0) return false -> 0

    pointers = pointers ++ Array.ofDim[Pointer](len)

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

  def copy()(implicit ctx: Context): Meta = {
    if(ctx.blocks.isDefinedAt(id)) return this

    val copy = new Meta(UUID.randomUUID.toString.getBytes(), MIN, MAX, LIMIT)

    ctx.blocks += copy.id -> copy
    ctx.parents += copy.id -> ctx.parents(id)

    copy.pointers = Array.ofDim[Pointer](length)
    copy.length = length
    copy.size = size

    for(i<-0 until length){
      val (k, c) = pointers(i)
      copy.setChild(k, c, i)
    }

    copy
  }

  def split()(implicit ctx: Context): Meta = {
    val right = new Meta(UUID.randomUUID.toString.getBytes(), MIN, MAX, LIMIT)

    ctx.blocks += right.id -> right

    val half = size/2
    val (bytes, len) = calcMaxLen(pointers, half)

    right.pointers = pointers.slice(len, length)
    right.length = right.pointers.length
    right.size = right.pointers.map{case (k, v) => k.length + v.length}.sum

    for(i<-0 until right.length){
      val (k, c) = right.pointers(i)
      right.setChild(k, c, i)
    }

    pointers = pointers.slice(0, len)
    length = pointers.length
    size = pointers.map{case (k, v) => k.length + v.length}.sum

    for(i<-0 until length){
      val (k, c) = pointers(i)
      setChild(k, c, i)
    }

    right
  }

  override def max: Option[B] = Some(pointers(length - 1)._1)

  override def isFull(): Boolean = size >= LIMIT
  override def isEmpty(): Boolean = size == 0
  override def hasMinimumSize(): Boolean = size >= MIN

  def inOrder(): Seq[Pointer] = {
    if(isEmpty()) return Seq.empty[Pointer]
    pointers.slice(0, length)
  }

}
