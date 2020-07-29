import org.apache.spark.sql.SparkSession
import java.io._
import scala.math.Ordering.Implicits._
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import util.control.Breaks._
import scala.math._
import scala.collection.Map
import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.json4s.jackson.Serialization.write
import org.json4s.JsonDSL._
import java.math.BigInteger

object task1 {
  def setone(s: String,hash_tables:scala.collection.immutable.IndexedSeq[((Int, Int), Int)]): List[Int] = {
      //val ss = s.map(_.toByte).sum
      //val byteString: ByteString = ByteString.copyFrom(DatatypeConverter.parseHexBinary(s))
      val ss = new BigInteger(s.toList.map(_.toInt.toHexString).mkString,16).remainder(BigInteger.valueOf(1000000000)).intValue
      val ans = HashSet.empty[Int]
      for (((a, b), c) <- hash_tables) {
        ans += (((ss * a + b) % c) % 8000).toInt
      }
      ans.toList
    }
  def main(args: Array[String]) {
    val t1 = System.currentTimeMillis
    val ss = SparkSession.builder().appName("scala").config("spark.master", "local[*]").getOrCreate()
    val sc = ss.sparkContext
    sc.setLogLevel("WARN")
    val dd = sc.textFile(args(0)).map(x => (parse(x) \\ "city").values.toString)//sc.textFile("../resource/asnlib/publicdata/business_first.json").map(x => (parse(x) \\ "city").values.toString) //sc.textFile(args(0)).map(x => (parse(x) \\ "city").values.toString)
    val random = new scala.util.Random()
    val nh = 2
    val a = for (i <- 1 to nh) yield -10000+random.nextInt(20000+1)
    val b = for (i <- 1 to nh) yield -10000+random.nextInt(20000+1)
    var c = List(804824051, 24670986, 300491)
    val hash_tables = sc.broadcast(a zip b zip c)
    val city = dd.distinct.collect.toSet

    val array_bit = dd.distinct.filter(x => x != "").flatMap(x=>setone(x,hash_tables.value)).distinct.collect().toSet
    var ans = ArrayBuffer[String]()
    val test = scala.io.Source.fromFile(args(1)).getLines.toList.map(x => (parse(x) \\ "city").values.toString)
    for (ci <- test) {
      if (ci != "") {
        val ss = new BigInteger(ci.toList.map(_.toInt.toHexString).mkString,16).remainder(BigInteger.valueOf(1000000000)).intValue
        //val ss = city.map(_.toByte).sum
        if ((for (((a, b), c) <- hash_tables.value) yield (((ss * a + b) % c) % 8000).toInt).toSet subsetOf array_bit) {
          ans += "1"
          //if (!city.contains(ci)) println("False positive")
        } else {
          ans += "0"
          if (city.contains(ci)) println("False Negative")
        }
      } else {
        ans += "1"
      }
    }
    val file = new File(args(2))
    val output = new BufferedWriter(new FileWriter(file))
    output.write(ans.mkString(" "))
    println(hash_tables.value)
    println("count: %s".format(94297-ans.map(_.toInt).sum))
    println("count of array bit: %s".format(array_bit.size))
    println("size of output: %s".format(ans.size))
    val t2 = System.currentTimeMillis
    println("Duration: %s".format((t2 - t1).toDouble / 1000))
  }

}
