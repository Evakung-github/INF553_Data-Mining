import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import java.io._
import scala.math.Ordering.Implicits._
import scala.collection.mutable.HashSet
import org.apache.spark.sql.Row

object task1 {
  def main(args: Array[String]) {
    val t1 = System.currentTimeMillis
    val ss = SparkSession.builder().appName("scala").config("spark.master", "local[*]").getOrCreate()
    val sc = ss.sparkContext
    sc.setLogLevel("WARN")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val thres = args(0).toInt
    val dd = sc.textFile(args(1)).mapPartitionsWithIndex { (idx, row) => if (idx == 0) row.drop(1) else row }.map(_.split(","))

    val b = dd.map(x => (x(1), x(0))).repartition(15).persist
    val edges = b.join(b).filter(x => x._2._1 != x._2._2).map(_.swap).aggregateByKey(HashSet.empty[String])((x, y) => x += y, (x, y) => x ++= y).filter(_._2.size >= thres).map(_._1)
    val vertices = sqlContext.createDataFrame(edges.flatMap(_.productIterator.toList).distinct().map(x => Tuple1(x.toString))).toDF("id")
    val g_edge = sqlContext.createDataFrame(edges).toDF("src", "dst")
    val g = GraphFrame(vertices, g_edge)
    val res = g.labelPropagation.maxIter(5).run()
    val ans = res.rdd.map(x => (x(1).toString, x(0).toString)).groupByKey.map((_._2.toArray.sorted)).sortBy(x => (x.size, x(0))).collect()

    val file = new File(args(2))
    val output = new BufferedWriter(new FileWriter(file))
    for (i <- ans) { output.write(i.map(x => "'%s'".format(x)).mkString(", ") + "\n") }

    output.close()
    val t2 = System.currentTimeMillis
    println("Duration: %s".format((t2 - t1).toDouble / 1000))

  }
}
