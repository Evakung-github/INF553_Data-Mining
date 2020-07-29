import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.json4s.jackson.Serialization.write
import org.json4s.JsonDSL._
import org.apache.spark.sql.SparkSession
import java.io._
import scala.collection.mutable.ListBuffer



object task2 {
  def main(args: Array[String]): Unit = {
    implicit val formats = org.json4s.DefaultFormats
    val spark = args(3)
    val n = args(4).toInt
    var ans = JObject()
    if (spark == "spark") {
      val ss = SparkSession
        .builder()
        .appName("scala")
        .config("spark.master", "local[*]")
        .getOrCreate()
      val sc = ss.sparkContext
      val review = sc.textFile(args(0))
      val business = sc.textFile(args(1))

      val bsn = business.map(x => parse(x)).filter(x => (x \\ "categories") != JNull).map(x => ((x \\ "business_id").values.toString, (x \\ "categories").values.toString.split(','))).flatMap(x => for (n <- x._2) yield { (x._1, n.trim()) })
      val rev = review.map(x => parse(x)).map(x => ((x \\ "business_id").values.toString, (x \\ "stars").values.toString))
      val a = bsn.join(rev).map(x => x._2).aggregateByKey((0.0, 0))((x: (Double, Int), y: String) => (x._1 + y.toFloat, x._2 + 1), (x: (Double, Int), y: (Double, Int)) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2).sortBy(key => (-key._2, key._1), ascending = true).take(n)
      var k = a.map(t => List(JString(t._1), JDouble(t._2))).toList

      ans = ans ~ ("result", k)
    } else {
      var review = new ListBuffer[List[String]]()
      for (line <- scala.io.Source.fromFile(args(0)).getLines) review += List((parse(line) \\ "business_id").values.toString, (parse(line) \\ "stars").values.toString)

      var business = new ListBuffer[List[String]]()
      for (line <- scala.io.Source.fromFile(args(1)).getLines) {
        if (parse(line) \\ "categories" != JNull) {
          business += List((parse(line) \\ "business_id").values.toString, (parse(line) \\ "categories").values.toString)
        }
      }

      var review_map = scala.collection.mutable.Map[String, Array[Double]]()
      for (r <- review) {
        if (review_map.contains(r(0))) {
          review_map(r(0))(0) = review_map(r(0))(0) + r(1).toDouble
          review_map(r(0))(1) = review_map(r(0))(1) + 1
        } else {
          review_map += (r(0) -> Array(r(1).toDouble, 1))

        }
      }

      var category_map = scala.collection.mutable.Map[String, Array[Double]]()
      for (b <- business) {
        if (review_map.contains(b(0)) & b(0) != JNull) {
          for (j <- b(1).split(',')) {

            if (!category_map.contains(j.trim())) {
              category_map += (j.trim() -> Array(0, 0))
            }
            category_map(j.trim())(0) = category_map(j.trim())(0) + review_map(b(0))(0)
            category_map(j.trim())(1) = category_map(j.trim())(1) + review_map(b(0))(1)
          }
        }
      }

      var ans_map = new ListBuffer[List[Any]]()
      for (i <- category_map.keys) {
        ans_map += List(i,category_map(i)(0) / category_map(i)(1))
      }

      ans = ans ~ ("result", ans_map.sortBy(x=>(-x(1).toString.toDouble,x(0).toString)).slice(0,n).map(t=>List(JString(t(0).toString),JDouble(t(1).toString.toDouble))))

    }

    val file = new File(args(2))
    val output = new BufferedWriter(new FileWriter(file))
    output.write(write(ans))
    output.close()
  }



}
