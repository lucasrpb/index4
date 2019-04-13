package index

import java.util.UUID

class Index(var root: Option[B],
            var size: Int,
            val SIZE: Int)(implicit ord: Ordering[B], store: Storage) {

  val MIN = SIZE/3
  val MAX = SIZE
  val LIMIT = 2 * MIN

  val MAX_KEY_SIZE = SIZE/8 - BLOCK_ID_SIZE

  implicit val ctx = new Context(store)

  def find(k: B, start: Option[B]): Option[Leaf] = {
    start match {
      case None => None
      case Some(id) => ctx.get(id).get match {
        case leaf: Leaf => Some(leaf)
        case meta: Meta =>

          val size = meta.length
          val pointers = meta.pointers

          for(i<-0 until size){
            val c = pointers(i)._2
            ctx.parents += c -> (Some(meta.id), i)
          }

          find(k, meta.findPath(k))
      }
    }
  }

  def find(k: B): Option[Leaf] = {
    if(root.isDefined){
      ctx.parents += root.get -> (None, 0)
    }

    find(k, root)
  }

  def fixRoot(p: Block): Boolean = {
    p match {
      case p: Meta =>

        if(p.length == 1){
          val c = p.pointers(0)._2
          root = Some(c)

          ctx.parents += c -> (None, 0)

          true
        } else {
          root = Some(p.id)

          ctx.parents += p.id -> (None, 0)

          true
        }

      case p: Leaf =>
        root = Some(p.id)

        ctx.parents += p.id -> (None, 0)

        true
    }
  }

  def recursiveCopy(p: Block): Boolean = {
    val (parent, pos) = ctx.parents(p.id)

    parent match {
      case None => fixRoot(p)
      case Some(pid) =>
        val PARENT = ctx.getMeta(pid).get.copy()
        PARENT.setChild(p.max.get, p.id, pos)
        recursiveCopy(PARENT)
    }
  }

  def insertEmpty(data: Seq[Tuple]): (Boolean, Int) = {
    val p = new Leaf(UUID.randomUUID.toString.getBytes(), MIN, MAX, LIMIT)

    val (ok, n) = p.insert(data)

    ctx.blocks += p.id -> p
    ctx.parents += p.id -> (None, 0)

    (ok && recursiveCopy(p)) -> n
  }

  def insertParent(left: Meta, prev: Block): Boolean = {

    val data = Seq(prev.max.get -> prev.id)

    if(left.isFull(data)){
      val right = left.split()

      if(ord.gt(prev.max.get, left.max.get)){
        right.insert(data)
      } else {
        left.insert(data)
      }

      return handleParent(left, right)
    }

    left.insert(data)

    recursiveCopy(left)
  }

  def handleParent(left: Block, right: Block): Boolean = {
    val (parent, pos) = ctx.parents(left.id)

    parent match {
      case None =>

        val meta = new Meta(UUID.randomUUID.toString.getBytes(), MIN, MAX, LIMIT)

        ctx.blocks += meta.id -> meta
        ctx.parents += meta.id -> (None, 0)

        meta.insert(Seq(
          left.max.get -> left.id,
          right.max.get -> right.id
        ))

        recursiveCopy(meta)

      case Some(pid) =>

        val PARENT = ctx.getMeta(pid).get.copy()
        PARENT.setChild(left.max.get, left.id, pos)

        insertParent(PARENT, right)
    }
  }

  def insertLeaf(leaf: Leaf, data: Seq[Tuple]): (Boolean, Int) = {
    val left = leaf.copy()

    if(left.isFull(data)){
      val right = left.split()
      return handleParent(left, right) -> 0
    }

    val (ok, n) = left.insert(data)

    (ok && recursiveCopy(left)) -> n
  }

  def insert(data: Seq[Tuple]): (Boolean, Int) = {

    val sorted = data.sortBy(_._1)
    val length = sorted.length
    var pos = 0

    for(i<-0 until length){
      val (k, v) = data(i)
      val bytes = k.length + v.length

      if(k.length > MAX_KEY_SIZE) return false -> 0
    }

    while(pos < length){

      var list = sorted.slice(pos, length)
      val (k, _) = list(0)

      val (ok, n) = find(k) match {
        case None => insertEmpty(list)
        case Some(leaf) =>

          val idx = list.indexWhere {case (k, _) => ord.gt(k, leaf.max.get)}
          if(idx > 0) list = list.slice(0, idx)

          insertLeaf(leaf, list)
      }

      if(!ok) return false -> 0

      pos += n
    }

    size += length

    true -> length
  }

}
