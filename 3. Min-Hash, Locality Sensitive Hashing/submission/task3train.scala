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

object task3train {
  def pearson_corr(iterators: Iterator[((Int, Int), Iterable[(Double, Double)])]): Iterator[Tuple3[Int, Int, Double]] = {
    //(6956,6246),Array(List(5.0, 5.0),List(5.0, 5.0))
    val ans = scala.collection.mutable.ArrayBuffer[Tuple3[Int, Int, Double]]()
    for (iter <- iterators) {
      val (b1, b2) = iter._1
      val (b1_score, b2_score) = iter._2.toArray.unzip

      val (b1_mean, b2_mean) = (b1_score.sum / b1_score.length, b2_score.sum / b2_score.length)
      val s = b1_score.map(_ - b1_mean)
      val t = b2_score.map(_ - b2_mean)
      val ss = s.map(x => math.pow(x, 2)).sum
      val st = t.map(x => math.pow(x, 2)).sum

      if (ss != 0 && st != 0) {
        ans += Tuple3(b1, b2, s.zip(t).map { case (x, y) => x * y }.sum.toDouble / math.sqrt(ss) / math.sqrt(st))
      }
    }
    ans.iterator
  }

  def main(args: Array[String]) {
    //<train_file> <model _file> <cf_type>
    val t1 = System.currentTimeMillis
    val ss = SparkSession
      .builder()
      .appName("scala")
      .config("spark.master", "local[*]")
      .getOrCreate()
    val sc = ss.sparkContext
    val dd = sc.textFile(args(0))
    //val dd = sc.textFile("train_review.json")
    val cf_type = args(2)

    implicit val formats = org.json4s.DefaultFormats
    val file = new File(args(1))

    val output = new BufferedWriter(new FileWriter(file))

    if (cf_type == "item_based") {
      val business = dd.map(x => ((parse(x) \\ "business_id").values.toString)).distinct
      val map_business = sc.broadcast((business.sortBy(x => x).collect() zip 0.until(business.count().toInt).toList).toMap)
      val map_business_inverse = sc.broadcast(map_business.value.map(_.swap))
      val u = dd.map(parse(_)).map(x => (x \\ "user_id".values.toString, (map_business.value((x \\ "business_id").values.toString), (x \\ "stars").values.toString.toDouble))).repartition(15).cache()
      val ans = u.join(u).filter(x => x._2._1._1 > x._2._2._1).map(x => ((x._2._1._1, x._2._2._1), (x._2._1._2, x._2._2._2))).groupByKey.filter(_._2.toArray.length >= 3).mapPartitions(pearson_corr(_)).filter(_._3 > 0).map(x => Map("b1" -> map_business_inverse.value(x._1), "b2" -> map_business_inverse.value(x._2), "sim" -> x._3)).collect()
     for (i <- ans) { output.write(write(i) + "\n") }

    } else {
      val user = dd.map(x => ((parse(x) \\ "user_id").values.toString)).distinct
      val map_user = sc.broadcast((user.sortBy(x => x).collect() zip 0.until(user.count().toInt).toList).toMap)
      val map_user_inverse = sc.broadcast(map_user.value.map(_.swap))
      val b = dd.map(parse(_)).map(x => (x \\ "business_id".values.toString, (map_user.value((x \\ "user_id").values.toString), (x \\ "stars").values.toString.toDouble))).repartition(15).cache()
      val ans = b.join(b).filter(x => x._2._1._1 > x._2._2._1).map(x => ((x._2._1._1, x._2._2._1), (x._2._1._2, x._2._2._2))).groupByKey.filter(_._2.toArray.length >= 3).mapPartitions(pearson_corr(_)).filter(_._3 > 0).map(x => Map("u1" -> map_user_inverse.value(x._1), "u2" -> map_user_inverse.value(x._2), "sim" -> x._3)).collect()
    for (i <- ans) { output.write(write(i) + "\n") }

    }
    output.close()
    val t2 = System.currentTimeMillis
    println("Duration: %s".format((t2 - t1).toDouble / 1000))



  }
}
