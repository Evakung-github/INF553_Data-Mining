import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.json4s.jackson.Serialization.write
import org.json4s.JsonDSL._
import org.apache.spark.sql.SparkSession
import java.io._
import scala.collection.mutable.ListBuffer
import org.apache.spark.HashPartitioner

object task3 {
  def main(args: Array[String]): Unit = {
    //val t0 = System.nanoTime()
    implicit val formats = org.json4s.DefaultFormats
    val ss = SparkSession
      .builder()
      .appName("scala")
      .config("spark.master", "local[*]")
      .getOrCreate()
    val sc = ss.sparkContext
    val review = sc.textFile(args(0))
    val partition_type = args(2)
    val n_partition = args(3).toInt
    val n = args(4).toInt
    var ans = JObject()
    if (partition_type == "default") {
      ans = ans ~ ("n_partitions", review.getNumPartitions)
      ans = ans ~ ("n_items", review.glom().map(x => x.length).collect().toList)
      val a = review.map(x => ((parse(x) \\ "business_id").values.toString, 1)).reduceByKey(_ + _).filter(_._2 > n).collect()
      val k = a.map(t => List(JString(t._1), JInt(t._2))).toList
      ans = ans ~ ("result", k)
    }else{
      ans = ans ~ ("n_partitions", n_partition)
      val cus = review.map(x => (parse(x) \\ "business_id").values.toString).map(x => (x,1)).partitionBy(new HashPartitioner(n_partition)).persist()
      ans = ans ~ ("n_items",cus.glom().map(_.length).collect().toList)
      val a = cus.reduceByKey(_ + _).filter(_._2 > n).collect()
      val k = a.map(t => List(JString(t._1), JInt(t._2))).toList
      ans = ans ~ ("result", k)
      
      
    }
    val file = new File(args(1))
    val output = new BufferedWriter(new FileWriter(file))
    output.write(write(ans))
    output.close()
    //val t1 = System.nanoTime()
    //println("Duration: ",(t1-t0)/10^6)
  }
}

