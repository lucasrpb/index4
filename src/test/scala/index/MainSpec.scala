package index

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

  val MAX_VALUE = Int.MaxValue

  def test(): Unit = {
    val rand = ThreadLocalRandom.current()

    val SIZE = 180//rand.nextInt(230, 1000)

    val ref = new AtomicReference(Ref())

    implicit val store = new MemoryStorage()

    def insert(): (Boolean, Seq[Tuple]) = {

      val old = ref.get()
      val index = new Index(old, SIZE)
      var list = Seq.empty[Tuple]

      val n = rand.nextInt(10, 200)

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphanumeric(1, 20).getBytes()//rand.nextInt(1, MAX_VALUE).toString.getBytes()

        //if(!list.exists{case (k1, _) => ord.equiv(k, k1)}){
          list = list :+ k -> k
        //}
      }

      val (ok, len) = index.insert(list)

      (ok && ref.compareAndSet(old, index.getRef()) && store.put(index.ctx.blocks)) -> list
    }

    var data = Seq.empty[Tuple]
    val n = 10

    for(i<-0 until n){
      insert() match {
        case (true, list) => data = data ++ list
        case _ =>
      }
    }

    val ldata = data.sortBy(_._1).map{case (k, v) => new String(k)}
    val idata = Query.inOrder(ref.get().root).map{case (k, v) => new String(k)}

    println(s"ldata ${ldata}\n")
    println(s"idata ${idata}\n")

    assert(idata.equals(ldata))
  }

  "index data " must "be equal to list data" in {

    val n = 1000

    for(i<-0 until n){
      test()
    }

  }

}
