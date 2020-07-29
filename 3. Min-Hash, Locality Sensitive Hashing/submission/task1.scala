import java.io._
import org.apache.spark.sql.SparkSession
import scala.math.Ordering.Implicits._
import util.control.Breaks._
import scala.util.control._
import scala.collection.mutable.ArrayBuffer
import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.json4s.jackson.Serialization.write
import org.json4s.JsonDSL._
import org.apache.spark.HashPartitioner

object task1 {
  def min_hash(iterators: Iterable[Int], hash_tables: IndexedSeq[(Int, Int)], r: Int): Iterator[Tuple2[Int, List[Int]]] = {

    val iters = iterators.toList
    var count = 0
    var ans = scala.collection.mutable.ArrayBuffer[Tuple2[Int, List[Int]]]()
    var group = scala.collection.mutable.ListBuffer[Int]()
    for ((a, b) <- hash_tables) {
      var min = Int.MaxValue
      for (i <- iters.toSeq) {
        min = Math.min(min, (i * a + b) % (26184 + 5))
      }
      group += min
      count += 1
      if (count % r == 0) {
        ans += Tuple2(count / r, group.toList)
        group = scala.collection.mutable.ListBuffer[Int]()
      }
    }
    ans.iterator
  }

  def create_Pair(iterators:Iterator[Array[Int]]):Iterator[List[Int]]={
    var ans = scala.collection.mutable.Set[List[Int]]()
    val iters = iterators.toList
    for (iter <- iters){
      for (i <- 0 to iter.length-1){
        for (j <- i + 1 to iter.length-1) {
          ans += List(iter(i), iter(j))
        }
      }
    }
    ans.iterator
  }

  def main(args: Array[String]) {
    val t1 = System.currentTimeMillis
    val ss = SparkSession
      .builder()
      .appName("scala")
      .config("spark.master", "local[*]")
      .getOrCreate()
    val sc = ss.sparkContext
    val dd = sc.textFile(args(0)).map(x=>((parse(x)\\"business_id").values.toString,(parse(x)\\"user_id").values.toString))
    val user = dd.map(_._2).distinct()
    val map_user = sc.broadcast((user.collect() zip 0.until(user.count().toInt).toList).toMap)
    val business = dd.map(_._1).distinct()
    val map_business = sc.broadcast((business.sortBy(x => x).collect() zip 0.until(business.count().toInt).toList).toMap)
    val map_business_inverse = sc.broadcast(map_business.value.map(_.swap))
    val data = dd.map(x => (map_business.value(x._1), map_user.value(x._2))).groupByKey.persist()

    val x = data.collectAsMap()
    val xx = sc.broadcast(x)

    val r = 1
    val nh = 45//99
    val random = new scala.util.Random()
    val a = for (i <- 1 to nh) yield random.nextInt(10000) - 5000
    val b = for (i <- 1 to nh) yield random.nextInt(10000) - 5000
    val hash_tables = sc.broadcast(a zip b)

    val similar_groups = data.flatMapValues(x => min_hash(x, hash_tables.value, r)).map(_.swap).groupByKey.filter(_._2.toSet.size > 1)
    val candidate_pairs = similar_groups.map(_._2.toArray.sorted).mapPartitions(create_Pair).distinct
    //println(("candidate count: ", candidate_pairs.count()))


    val ans =candidate_pairs.map(x => (x, (xx.value(x(0)).toSet & xx.value(x(1)).toSet).size.toDouble / (xx.value(x(0)).toSet | xx.value(x(1)).toSet).size.toDouble)).filter(_._2 >= 0.05).map(x => Map("b1"->map_business_inverse.value(x._1(0)),"b2"-> map_business_inverse.value(x._1(1)),"sim"-> x._2)).collect()
    //println(("ans count: ", ans.size))
    implicit val formats = org.json4s.DefaultFormats
    val file = new File(args(1))

    val output = new BufferedWriter(new FileWriter(file))
    for (i <- ans) { output.write(write(i) + "\n") }

    output.close()
    val t2 = System.currentTimeMillis
    println("Duration: %s".format((t2 - t1).toDouble / 1000))

  }
}

                