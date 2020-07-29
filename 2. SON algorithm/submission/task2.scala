import java.io._
import org.apache.spark.sql.SparkSession
import scala.math.Ordering.Implicits._
import util.control.Breaks._
import scala.util.control._
import scala.collection.mutable.ArrayBuffer

object task2 {
  def A_priori(iterators: Iterator[(String, Iterable[String])], support: Int): Iterator[Array[Set[String]]] = {
    var count = scala.collection.mutable.Map[Set[String], Int]()
    var candidate = scala.collection.mutable.Map[Int, Array[Set[String]]]()
    val iters = iterators.toList

    for (i <- iters) {
      for (j <- i._2) {
        if (count.contains(Set(j))) {
          count(Set(j)) += 1
        } else {
          count(Set(j)) = 1
        }
      }
    }
    var prev = scala.collection.mutable.ArrayBuffer[Set[String]]()
    for (i <- count.filter(_._2 >= support).keys) prev += i
    var ans = scala.collection.mutable.ArrayBuffer[Array[Set[String]]]()
    
    var c_items = 2
    while (prev.size > 0) {
      count.clear
      ans += prev.toArray
      for (i <- 0 to prev.size - 1) {
        for (j <- i + 1 to prev.size - 1) {
          if ((prev(i) & prev(j)).size == c_items - 2) {
            count(prev(i) | prev(j)) = 0
          }
        }
      }
      //ans += count.keys.toArray
      val inloop = new Breaks
      //ans.iterator
      prev = ArrayBuffer[Set[String]]()
      for (cand <- count.keys) {
        var c = 0
        inloop.breakable {
          for (i <- iters) {
            if (cand subsetOf i._2.toSet) {
              
              c += 1
              if (c >= support) {
                prev += cand
                inloop.break
              }
            }
          }
        }
      }
      c_items += 1
    }
    ans.iterator
  }

  def partition_red(iterators: Iterator[(String, Iterable[String])], candidates: Set[Set[String]]): Iterator[(Set[String], Int)] = {
    var count = scala.collection.mutable.Map[Set[String], Int]()
    for (i <- candidates) count(i) = 0
    for (i <- iterators) {
      for (cand <- candidates) {
        if (cand subsetOf i._2.toSet) count(cand) += 1
      }
    }
    count.iterator
  }

  def main(args: Array[String]) {
    val t1 = System.currentTimeMillis
    val ss = SparkSession
      .builder()
      .appName("scala")
      .config("spark.master", "local[*]")
      .getOrCreate()
    val sc = ss.sparkContext

    val threshold = args(0).toInt
    val support = args(1).toInt
    val dd = sc.textFile(args(2)).mapPartitionsWithIndex { (idx, row) => if (idx == 0) row.drop(1) else row }
    val n_partition = dd.getNumPartitions
    val sp = (support + (n_partition - 1)) / n_partition
    val data = dd.map((x) => (x.split(",")(0), x.split(",")(1))).groupByKey.mapValues(_.toSet).filter(x => x._2.size > threshold)
    val candidates = data.mapPartitions(x => A_priori(x, sp)).flatMap(x => x).distinct
    val ans1 = candidates.map(x => (x.size, x.toSeq.sorted)).groupByKey.mapValues(x => x.toArray.sorted).sortBy(x => x._1).collect()

    val c = candidates.collect().toSet

    val frequent = data.mapPartitions(x => partition_red(x, c)).reduceByKey(_ + _).filter(_._2 >= support).map(x => (x._1.size, x._1.toSeq.sorted)).groupByKey.mapValues(x => x.toArray.sorted).sortBy(x => x._1).collect()

    val file = new File(args(3))
    val output = new BufferedWriter(new FileWriter(file))
    output.write("Candidates:\n")
    for (i <- ans1) { output.write(i._2.map(x => x.mkString("', '")).map(x => "('%s')".format(x)).mkString(",") + "\n\n") }
    output.write("Frequent Itemsets:\n")
    for (i <- frequent) { output.write(i._2.map(x => x.mkString("', '")).map(x => "('%s')".format(x)).mkString(",") + "\n\n") }

    output.close()
    val t2 = System.currentTimeMillis
    println("Duration: %s".format((t2 - t1).toDouble / 1000))

  }

}

