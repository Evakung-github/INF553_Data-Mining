import java.io._
import org.json4s.jackson.JsonMethods._
import scala.math._
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.Serialization.write

object task2predict {


  def cos_sim(iterators: Map[String, String], user: Map[String,Any], bus: Seq[String]): Tuple3[String, String, Double] = {
    var mul: Double = 0.0

    val up = user.mapValues(_.toString.toDouble)
    for (i <- bus) {
      if (user.contains(i)) {
        mul += up(i)
      }
    }
    val t = sqrt(up.map(x => pow(x._2, 2)).sum)

    return (iterators("user_id"), iterators("business_id"), mul/sqrt(bus.length.toDouble)/t)

  }

  def main(args: Array[String]) {
    val t1 = System.currentTimeMillis
    val ss = SparkSession
      .builder()
      .appName("scala")
      .config("spark.master", "local[*]")
      .getOrCreate()
    val sc = ss.sparkContext
    val valid = sc.textFile(args(0))
    val read = scala.io.Source.fromFile(args(1)).getLines

    val business_profile = sc.broadcast(parse(read.next).values.asInstanceOf[Map[String,Seq[String]]])
    val t = parse(read.next).values.asInstanceOf[Map[String,Map[String,Any]]]
    println("t")
    val user_profile = sc.broadcast(t)
    println("user_profile")


    val ans = valid.map(x => parse(x).values.asInstanceOf[Map[String, String]]).filter(x => user_profile.value.contains(x("user_id")) && business_profile.value.contains(x("business_id"))).map(x => cos_sim(x, user_profile.value(x("user_id")), business_profile.value(x("business_id")))).filter(_._3 >= 0.01).map(x => Map("user_id" -> x._1, "business_id" -> x._2, "sim" -> x._3)).collect()
    implicit val formats = org.json4s.DefaultFormats
    val file = new File(args(2))

    val output = new BufferedWriter(new FileWriter(file))
    for (i <- ans) { output.write(write(i) + "\n") }

    output.close()

    val t2 = System.currentTimeMillis
    println("Duration: %s".format((t2 - t1).toDouble / 1000))

  }
}
              