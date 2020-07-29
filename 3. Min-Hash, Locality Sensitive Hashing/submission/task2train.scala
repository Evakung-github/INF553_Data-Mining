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
import scala.math._

object task2train{

  def tf (iterators:Iterable[Array[String]],idf: scala.collection.Map[String,Double]): Seq[String] = {
    val iters = iterators.flatten
    val xx = iters.groupBy(identity).mapValues(_.size.toDouble)
    val memo = scala.collection.mutable.Map(xx.toSeq:_*)
    val max_ = memo.maxBy(_._2)._2
    for (i<-memo.keys) memo(i) = memo(i).toDouble/max_ * idf(i)
    return memo.toSeq.sortBy(-_._2).map(_._1).slice(0,200)
  }

  def user_count(iterators: Iterable[Array[String]]):scala.collection.immutable.Map[String,Double]={
    val iters = iterators.flatten
    val n = iters.size
    val xx = iters.groupBy(identity).mapValues(_.size.toDouble)
    val memo = scala.collection.mutable.Map(xx.toSeq: _*)
    for (i<-memo.keys) memo(i) = memo(i)/n
    return memo.toArray.sortBy(-_._2).slice(0,300).toMap
  }


  def main(args: Array[String]) {
    val t1 = System.currentTimeMillis
    val ss = SparkSession
      .builder()
      .appName("scala")
      .config("spark.master", "local[*]")
      .getOrCreate()
    val sc = ss.sparkContext

    val dd = sc.textFile(args(0))
    val stopwords = scala.io.Source.fromFile(args(2)).getLines.toSet

    val business = dd.map(x=>((parse(x)\\"business_id").values.toString,(parse(x)\\"text").values.toString.toLowerCase().split("[^a-zA-Z]+"))).mapValues(x=> x.filter(!stopwords.contains(_)))
    val rare_words = sc.broadcast(business.flatMap(_._2).map(x=>(x,1)).reduceByKey(_+_).filter(_._2<=30).map(_._1).collect().toSet)

    val business_filter = business.mapValues(x=> x.filter(!rare_words.value.contains(_))).groupByKey
    val bus_N = business_filter.count()
    val idf = sc.broadcast(business_filter.flatMap(_._2.flatten.toSet).map(x=>(x,1)).reduceByKey(_+_).mapValues(x=>log10(bus_N/x)/log10(2)).collectAsMap())


    implicit val formats = org.json4s.DefaultFormats
    val file = new File(args(1))
    val output = new BufferedWriter(new FileWriter(file))



    val features = sc.broadcast(business_filter.mapValues(x=>tf(x,idf.value)).flatMap(_._2).distinct.collect().toSet)
    println("features here")
    output.write(write(business_filter.mapValues(x=>tf(x,idf.value)).collectAsMap())+"\n")
    business_filter.unpersist()
    println("business_profile")
    val user =   dd.map(x => ((parse(x) \\ "user_id").values.toString, (parse(x) \\ "text").values.toString.toLowerCase().split("[^a-zA-Z]+"))).mapValues(x => x.filter(features.value.contains(_)))
    //user.collect()

    output.write(write(user.groupByKey.mapValues(user_count).collectAsMap())+"\n")
    println("user here")

    output.close()


    val t2 = System.currentTimeMillis

    println("Duration: %s".format((t2 - t1).toDouble / 1000))

    }

}
                                                          