package index

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

import com.google.common.primitives.UnsignedBytes
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.FlatSpec

class MainSpec extends FlatSpec {

  implicit val ord = new Ordering[Array[Byte]]{
    val comp = UnsignedBytes.lexicographicalComparator()
    override def compare(x: Array[Byte], y: Array[Byte]): Int = comp.compare(x, y)
  }

  def test(): Unit = {
    val rand = ThreadLocalRandom.current()

    val SIZE = rand.nextInt(400, 2048)

    implicit val store = new MemoryStorage()

    val ref = new AtomicReference[(Option[B], Int)]((None, 0))
    var data = Seq.empty[Tuple]

    def insert(): Unit = {

      val old = ref.get()
      val index = new Index(old._1, old._2, SIZE)
      var list = Seq.empty[Tuple]

      val n = rand.nextInt(10, 100)

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphanumeric(1, index.MAX_KEY_SIZE).getBytes()
        val v = UUID.randomUUID.toString.getBytes()

        if(!list.exists{case (k1, v) => ord.equiv(k, k1)}){
          list = list :+ k -> v
        }
      }

      val (ok, _) = index.insert(list)

      if(ok && ref.compareAndSet(old, (index.root, index.size))){
        data = data ++ list
        store.put(index.ctx.blocks)
      }
    }

    val n = 10

    for(i<-0 until n){
      insert()
    }

    val ldata = data.sortBy(_._1).map{case (k, v) => new String(k)}
    val idata = Query.inOrder(ref.get()._1).map{case (k, v) => new String(k)}

    println(s"SIZE ${SIZE}\n")
    println(s"list data $ldata size ${ldata.size}\n")
    println(s"index data ${idata} size ${idata.size}\n")

    assert(idata.equals(ldata))
  }

  "index data " must "be equal to list data" in {

    val n = 1000

    for(i<-0 until n){
      test()
    }

  }

}
