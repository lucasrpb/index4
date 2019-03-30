package index

import java.util.UUID

class Index(val ref: Ref,
            val SIZE: Int)(implicit val ord: Ordering[B], store: Storage) {

  val MAX_KEY_SIZE = 20
  val BLOCK_ADDR_SIZE = 36

  val LEAF_MIN = (20 * SIZE)/100
  val LEAF_MAX = SIZE
  val LEAF_LIMIT = (80 * SIZE)/100

  val META_MIN = (20 * SIZE)/100 + BLOCK_ADDR_SIZE
  val META_MAX = 5 * META_MIN
  val META_LIMIT = (80 * META_MAX)/100

  val MAX_TUPLE_SIZE = LEAF_MIN

  var root = ref.root
  var size = ref.size

  println(s"LEAF_MIN $LEAF_MIN LEAF_MAX $LEAF_MAX LEAF_LIMIT $LEAF_LIMIT\n")
  println(s"META_MIN $META_MIN META_MAX $META_MAX META_LIMIT $META_LIMIT\n")

  implicit val ctx = new Context(store)

  def getRef() = Ref(root, size)

  def find(k: B, start: Option[B]): Option[Leaf] = {
    start match {
      case None => None
      case Some(start) => ctx.get(start).get match {
        case leaf: Leaf => Some(leaf)
        case meta: Meta =>

          val length = meta.length
          val pointers = meta.pointers

          for(i<-0 until length){
            val child = pointers(i)._2
            ctx.parents += child -> (Some(meta.id), i)
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

        if(p.size == 1){
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
      case Some(id) =>
        val PARENT = ctx.getMeta(id).get.copy()
        PARENT.setChild(p.max.get, p.id, pos)
        recursiveCopy(PARENT)
    }
  }

  def insertEmptyIndex(data: Seq[Tuple]): (Boolean, Int) = {
    val p = new Leaf(UUID.randomUUID.toString.getBytes(), LEAF_MIN, LEAF_MAX, LEAF_LIMIT)

    val (ok, n) = p.insert(data)

    ctx.blocks += p.id -> p
    ctx.parents += p.id -> (None, 0)

    (ok && recursiveCopy(p)) -> n
  }

  def insertParent(left: Meta, prev: Block): Boolean = {

    if(left.isFull()){

      val right = left.split()

      if(ord.gt(prev.max.get, left.max.get)){
        right.insert(Seq(prev.max.get -> prev.id))
      } else {
        left.insert(Seq(prev.max.get -> prev.id))
      }

      return handleParent(left, right)
    }

    left.insert(Seq(prev.max.get -> prev.id))

    recursiveCopy(left)
  }

  def handleParent(left: Block, right: Block): Boolean = {
    val (parent, pos) = ctx.parents(left.id)

    parent match {
      case None =>

        val meta = new Meta(UUID.randomUUID.toString.getBytes(), META_MIN, META_MAX, META_LIMIT)

        ctx.blocks += meta.id -> meta
        ctx.parents += meta.id -> (None, 0)
        meta.insert(Seq(
          left.max.get -> left.id,
          right.max.get -> right.id
        ))

        recursiveCopy(meta)

      case Some(id) =>

        val PARENT = ctx.getMeta(id).get.copy()
        PARENT.setChild(left.max.get, left.id, pos)

        insertParent(PARENT, right)
    }
  }

  def insertLeaf(leaf: Leaf, data: Seq[Tuple]): (Boolean, Int) = {
    val left = leaf.copy()

    if(leaf.isFull()){
      val right = left.split()
      return handleParent(left, right) -> 0
    }

    val (ok, n) = left.insert(data)

    (ok && recursiveCopy(left)) -> n
  }

  def insert(data: Seq[Tuple]): (Boolean, Int) = {

    val sorted = data.sortBy(_._1)
    val size = sorted.length
    var pos = 0

    for(i<-0 until size){
      val (k, v) = data(i)
      val bytes = k.length + v.length

      if(bytes > MAX_TUPLE_SIZE || k.length > MAX_KEY_SIZE) return false -> 0
    }

    while(pos < size){

      var list = sorted.slice(pos, size)
      val (k, _) = list(0)

      val (ok, n) = find(k) match {
        case None => insertEmptyIndex(list)
        case Some(leaf) =>

          val idx = list.indexWhere {case (k, _) => ord.gt(k, leaf.max.get)}
          if(idx > 0) list = list.slice(0, idx)

          insertLeaf(leaf, list)
      }

      if(!ok) return false -> 0

      pos += n
    }

    this.size += size

    true -> size
  }

}
