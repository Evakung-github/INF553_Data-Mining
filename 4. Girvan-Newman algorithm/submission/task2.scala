import org.apache.spark.sql.SparkSession
import java.io._
import scala.math.Ordering.Implicits._
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import util.control.Breaks._
import scala.math._
import scala.collection.Map
object task2 {

  def main(args: Array[String]) {

    def bfs_eachnode(n: String, mapadj: scala.collection.mutable.Map[String, ListBuffer[String]]): Iterator[Tuple2[(String, String), Double]] = {
      val ans = ArrayBuffer[Tuple2[(String, String), Double]]()
      var cur = HashSet.empty[String]
      cur += n
      var parent = scala.collection.mutable.Map[String, ArrayBuffer[String]]()
      var cn_sp = scala.collection.mutable.Map[String, Int]()
      cn_sp(n) = 1
      parent(n) = ArrayBuffer.empty[String]
      var level = ArrayBuffer[HashSet[String]]()
      var next = HashSet.empty[String]
      while (cur.size > 0) {
        next.clear
        for (c <- cur) {
          for (node <- mapadj(c)) {
            //println(("here:",c))
            breakable {
              if (next.contains(node)) {
                parent(node) += c
                cn_sp(node) += cn_sp(c)
              } else if (parent.contains(node)) break
              else {
                next += node
                parent(node) = ArrayBuffer[String](c)
                cn_sp(node) = cn_sp(c)
              }
            }
          }
        }
        level += next.clone
        cur = next.clone
      }
      var betweenness = scala.collection.mutable.Map[(String, String), Double]()
      var node_value = scala.collection.mutable.Map[String, Double]()
      var credit = 0.0
      for (l <- (0 to level.size - 1).reverse) {
        for (node <- level(l)) {
          if (!node_value.contains(node)) node_value(node) = 1.0
          for (p <- parent(node)) {
            credit = node_value(node).toDouble * cn_sp(p).toDouble / cn_sp(node)
            val key = List(node, p).sorted(Ordering.String) match { case List(a, b) => (a, b) }
            betweenness(key) = credit
            if (node_value.contains(p)) { node_value(p) += credit }
            else { node_value(p) = 1 + credit }
          }
        }
      }
      betweenness.toSeq.iterator
    }
    val t1 = System.currentTimeMillis
    val ss = SparkSession.builder().appName("scala").config("spark.master", "local[*]").getOrCreate()
    val sc = ss.sparkContext
    sc.setLogLevel("WARN")
    val thres = args(0).toInt
    val dd = sc.textFile(args(1)).mapPartitionsWithIndex { (idx, row) => if (idx == 0) row.drop(1) else row }.map(_.split(","))

    val b = dd.map(x => (x(1), x(0))).repartition(15).persist
    val edges = b.join(b).filter(x => x._2._1 != x._2._2).map(_.swap).aggregateByKey(HashSet.empty[String])((x, y) => x += y, (x, y) => x ++= y).filter(_._2.size >= thres).map(_._1)
    val vertices = edges.flatMap(_.productIterator.toList).distinct()
    //val ori_adj = sc.broadcast(edges.repartition(30).groupByKey.mapValues(_.toList).collectAsMap())
    val ori_adj = sc.broadcast(scala.collection.mutable.Map(edges.repartition(30).groupByKey.mapValues(collection.mutable.ListBuffer.empty ++= _).collect().toSeq: _*))

    val temp = vertices.flatMap(x => bfs_eachnode(x.toString, ori_adj.value)).reduceByKey(_ + _).mapValues(_.toDouble / 2)
    var between = collection.mutable.Map(temp.collectAsMap().toSeq: _*)
    val ans1 = between.toSeq.sortBy(x => (-x._2, x._1._1, x._1._2))
    val file = new File(args(2))
    val output = new BufferedWriter(new FileWriter(file))
    for (i <- ans1) { output.write("(" + i._1.productIterator.toList.map(x => "'%s'".format(x)).mkString(", ") + "), " + i._2.toString + "\n") }
    output.close

    val map_adj = collection.mutable.Map(ori_adj.value.toSeq: _*).map(x => (x._1, collection.mutable.ListBuffer.empty ++= x._2))

    val ori_degree = sc.broadcast(map_adj.map(x => (x._1, x._2.size)))

    def connected_components(c: HashSet[String]): collection.mutable.ListBuffer[HashSet[String]] = {
      def traverse(x: String, visited: HashSet[String]): HashSet[String] = {
        for (i <- map_adj(x)) {
          if (!visited.contains(i)) {
            visited += i
            visited ++= traverse(i, visited)
          }

        }
        visited
      }

      var cc = collection.mutable.ListBuffer[HashSet[String]]()
      var visited = HashSet.empty[String]
      for (i <- c) {
        if (!visited.contains(i)) {
          val temp = traverse(i, HashSet.empty[String])
          cc += temp
          visited ++= temp
        }
      }
      cc
    }
    val m = edges.count()
    var cc = connected_components(HashSet(map_adj.keys.toSeq: _*))
    var mol = scala.collection.mutable.Map[Set[String], Double]()

    def calModularity(x: Set[String]): Double = {
      if (mol.contains(x)) mol(x)
      else {
        var mm = 0.0
        var self = 0.0
        val y = x.toList
        for (i <- 0 to y.size - 1) {
          self += (ori_degree.value(y(i)).toDouble * ori_degree.value(y(i)).toDouble / m)
          for (j <- i + 1 to y.size - 1) {
            if (ori_adj.value(y(i)).contains(y(j))) mm += (1 - ori_degree.value(y(i)).toDouble * ori_degree.value(y(j)).toDouble / m)
            else mm += (-ori_degree.value(y(i)).toDouble * ori_degree.value(y(j)).toDouble / m)
          }
        }
        mm*2 - self
      }
    }


    val temp_mol = sc.parallelize(cc).map(x => (x.toSet, calModularity(x.toSet))).collectAsMap()
    mol = scala.collection.mutable.Map(temp_mol.toSeq: _*)
    var ans2 = collection.mutable.ListBuffer[HashSet[String]]()
    var max_mol = mol.values.sum.toDouble / m
    println(("Initial modularity: ", max_mol))
    while (between.size > 0) {
      val (remove_edge, z) = between.maxBy(_._2)
      map_adj(remove_edge._1) -= remove_edge._2
      map_adj(remove_edge._2) -= remove_edge._1
      //val k = map_adj.mapValues(_.toList).asInstanceOf[Map[String,List[String]]]
      between.remove(remove_edge)
      between.size
      var ii = HashSet.empty[String]
      breakable {
        for (i <- cc) {
          if (i.contains(remove_edge._1)) {
            val new_between = sc.parallelize(i.toList).flatMap(x => bfs_eachnode(x.toString, map_adj)).reduceByKey(_ + _).mapValues(_.toDouble / 2).collect()
            for ((e, v) <- new_between) between(e) = v
            ii = i
            break
          }
        }
      }
      val new_cc = connected_components(ii)
      if (new_cc.size > 1) {
        cc -= ii
        cc ++= new_cc
        val temp_mol = sc.parallelize(cc).map(x => calModularity(x.toSet)).sum.toDouble / m
        if (temp_mol > max_mol) {
          ans2 = cc.clone
          max_mol = temp_mol
          println(temp_mol)
        }

      }

    }
    val ans = sc.parallelize(ans2).map(_.toArray.sorted).sortBy(x => (x.size, x(0))).collect()

    val file2 = new File(args(3))
    val output2 = new BufferedWriter(new FileWriter(file2))
    for (i <- ans) { output2.write(i.map(x => "'%s'".format(x)).mkString(", ") + "\n") }

    output2.close()
    val t2 = System.currentTimeMillis
    println("Duration: %s".format((t2 - t1).toDouble / 1000))

  }
}
