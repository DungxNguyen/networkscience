package example

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object HelloGraphX {
  val spark = SparkSession.builder.appName("Hello Graph X").master("local").getOrCreate()
  val sparkContext = spark.sparkContext

  def main(args: Array[String]): Unit = {
    println("Hello Graph X")
    val vertices = sparkContext.parallelize(Array((1L, 1), (2L, 2), (3L, 3), (4L, 4), (5L, 5)))
    val edges = sparkContext.parallelize(Array(Edge(1, 2, 1), Edge(2, 3, 1), Edge(3, 4, 1), Edge(4, 5, 1)))
    var graph: Graph[Int, Int] = Graph(vertices, edges)

    graph.pageRank(0.00001).vertices.collect.foreach(print)
  }
}