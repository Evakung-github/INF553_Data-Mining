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
import scala.collection.Map
import scala.math._

object task3predict {
  def predict_item(iterators: Iterator[((String, String), Iterable[(String, Double)])], model_dict: Map[(String, String), Double]): Iterator[Tuple3[String, String, Double]] = {
    val ans = scala.collection.mutable.ArrayBuffer[Tuple3[String, String, Double]]()
    for (iter <- iterators) {
      val (active_user, pred_item) = iter._1
      val iter_ = iter._2.toList
      val f = scala.collection.mutable.ArrayBuffer[(Double, Double)]()
      val T = iter_.length
      val avg = iter_.map(_._2).sum.toDouble / T
      for ((sim, score) <- iter_) {
        val key = List(pred_item, sim).sorted(Ordering.String.reverse) match { case List(a, b) => (a, b) }
        if (model_dict.contains(key)) {
          f += (List(model_dict(key), score) match { case List(a, b) => (a, b) })

        }
      }
      val top = 5
      val ff = f.sortBy(_._1)(Ordering.Double.reverse).slice(0, top)
      if (ff.length != 0) {
        val frac = ff.length.toDouble / T
        val pred_score = ff.map { case (x, y) => x * y }.sum.toDouble / ff.map(_._1).sum
        ans += Tuple3(active_user, pred_item, pred_score * frac + avg * (1 - frac))
      }
    }
    ans.iterator
  }

  def predict_user(iterators: Iterator[((String, String, Double), Iterable[(String, Double)])], model_dict: Map[(String, String), Double], user_avg: Map[String, Double]): Iterator[Tuple3[String, String, Double]] = {
    val ans = scala.collection.mutable.ArrayBuffer[Tuple3[String, String, Double]]()
    for (iter <- iterators) {
      val (pred_item, active_user, frac) = iter._1
      val iter_ = iter._2.toList
      val f = scala.collection.mutable.ArrayBuffer[(Double, Double)]()
      val T = iter_.length
      val avg = iter_.map(_._2).sum.toDouble / T
      for ((sim, score) <- iter_) {
        val key = List(active_user, sim).sorted(Ordering.String.reverse) match { case List(a, b) => (a, b) }
        if (model_dict.contains(key)) {
          f += (List(model_dict(key), score - user_avg(sim)) match { case List(a, b) => (a, b) })
        }
      }
      val top = 5
      val ff = f.sortBy(_._1)(Ordering.Double.reverse).slice(0, top)
      if (ff.length != 0) {
        val pred_score = ff.map { case (x, y) => x * y }.sum.toDouble / ff.map(_._1).sum
        ans += Tuple3(active_user, pred_item, user_avg(active_user) + pred_score * math.min(frac, 1))
      }
    }
    ans.iterator
  }

  def main(args: Array[String]) {
    //<train_file> <test_file> <model_file> <output_file> <cf_type>
    val t1 = System.currentTimeMillis
    val ss = SparkSession
      .builder()
      .appName("scala")
      .config("spark.master", "local[*]")
      .getOrCreate()
    val sc = ss.sparkContext
    val dd = sc.textFile(args(0))
    val model = sc.textFile(args(2))
    val cf_type = args(4)
    //implicit val formats = org.json4s.DefaultFormats
    val file = new File(args(3))
    val output = new BufferedWriter(new FileWriter(file))
    if (cf_type == "item_based") {
      val valid = sc.textFile(args(1)).map(x => ((parse(x) \\ "user_id").values.toString, (parse(x) \\ "business_id").values.toString)).partitionBy(new HashPartitioner(2))
      val user_business = dd.map(parse(_)).map(x => ((x \\ "user_id").values.toString, ((x \\ "business_id").values.toString, (x \\ "stars").values.toString.toDouble)))
      val model_dict = sc.broadcast(model.map(parse(_)).map(x => (((x \\ "b1").values.toString, (x \\ "b2").values.toString), (x \\ "sim").values.toString.toDouble)).collectAsMap())
      val ans = valid.join(user_business).map(x => ((x._1, x._2._1), x._2._2)).groupByKey.mapPartitions(x => predict_item(x, model_dict.value)).map(x => Map("user_id" -> x._1, "business_id" -> x._2, "stars" -> x._3)).collect()

      implicit val formats = org.json4s.DefaultFormats
      for (i <- ans) { output.write(write(i) + "\n") }
    } else {
      val valid = sc.textFile(args(1)).map(x => ((parse(x) \\ "business_id").values.toString, (parse(x) \\ "user_id").values.toString)).partitionBy(new HashPartitioner(2))
      val business_user = dd.map(parse(_)).map(x => ((x \\ "business_id").values.toString, ((x \\ "user_id").values.toString, (x \\ "stars").values.toString.toDouble)))
      val user_avg = sc.broadcast(dd.map(parse(_)).map(x => ((x \\ "user_id").values.toString, (x \\ "stars").values.toString.toDouble)).aggregateByKey((0.0, 0))((x,y)=>(x._1 + y, x._2 + 1), (x,y)=>(x._1 + y._1, x._2 + y._2)).mapValues(x => x._1.toDouble / x._2).collectAsMap())
      val n_user = user_avg.value.size.toDouble
      val n_user_per_item = sc.broadcast(dd.map(parse(_)).map(x => ((x \\ "business_id").values.toString, 1)).reduceByKey(_+_).collectAsMap())
      val model_dict = sc.broadcast(model.map(parse(_)).map(x => (((x \\ "u1").values.toString, (x \\ "u2").values.toString), (x \\ "sim").values.toString.toDouble)).collectAsMap())
      val ans = valid.join(business_user).map(x => ((x._1, x._2._1, log10(n_user / n_user_per_item.value(x._1)) / log10(1000)), x._2._2)).groupByKey.mapPartitions(x => predict_user(x, model_dict.value, user_avg.value)).map(x => Map("user_id" -> x._1, "business_id" -> x._2, "stars" -> x._3)).collect()
      implicit val formats = org.json4s.DefaultFormats
      for (i <- ans) { output.write(write(i) + "\n") }
    }
    output.close()
    val t2 = System.currentTimeMillis
    println("Duration: %s".format((t2 - t1).toDouble / 1000))

  }
}
                                                     