//package homework

import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.json4s.jackson.Serialization.write
import org.json4s.JsonDSL._
import org.apache.spark.sql.SparkSession
import java.io._

object task1 {

  def main(args:Array[String]): Unit = { 
    implicit val formats = org.json4s.DefaultFormats
    val ss = SparkSession
        .builder()
        .appName("scala")
        .config("spark.master", "local[*]")
        .getOrCreate()
    val sc = ss.sparkContext
    val t = sc.textFile(args(0))
    val stopwords = scala.io.Source.fromFile(args(2)).getLines.toList
    val y = args(3)
    val m = args(4).toInt
    val n = args(5).toInt
    var ans = JObject()

    
    ans = ans ~ ("A", t.count())
    ans = ans ~ ("B", t.map(x => parse(x)).filter(x => (x \ "date").values.toString.slice(0,4) == y).count())
    ans = ans ~ ("C", t.map(x => parse(x)).map(x => (x \ "user_id").values.toString).distinct().count())
    var a = t.map(x => parse(x)).map(x => ((x \ "user_id").values.toString, 1)).reduceByKey((x, y) => x + y).sortBy(key => (-key._2, key._1), ascending = true).take(m)
    var k = a.map(t => List(JString(t._1), JInt(t._2))).toList
    ans = ans ~ ("D", k)
    
    a = t.map(x => parse(x)).flatMap(x => (x \ "text").values.toString.split("[ \\(\\[,.!?\n:;\\]\\)]+")).filter(x => !stopwords.contains(x.toLowerCase())).map(x=>(x.toLowerCase().trim(),1)).reduceByKey((x, y) => x + y).sortBy(key => (-key._2, key._1), ascending = true).take(n)
    //k = a.map(t => JString(t)).toList
    ans = ans ~ ("E", a.map(x=>x._1).toList)

    val file = new File(args(1))
    val output = new BufferedWriter(new FileWriter(file))
    output.write(write(ans))
    output.close()
  }
}

