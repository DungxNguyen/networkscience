package example

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession
import java.nio.file.Paths
import scala.annotation.tailrec
import org.apache.spark.graphx.EdgeDirection

object PageRank {

  val spark = SparkSession.builder.appName("Page Rank").master("local").getOrCreate()
  val sparkContext = spark.sparkContext

  def main(args: Array[String]): Unit = {
    println("Page Rank", args(0))

    val originalGraph = loadGraph(args(0))
    println("# vertices: " + originalGraph.numVertices)
    println("# edges: " + originalGraph.numEdges)

    val myPageRank = pageRank(originalGraph).vertices.sortBy{case (id, value) => -value}.take(10)
    
    val sparkPageRank = originalGraph.pageRank(0.00001).vertices.sortBy{case (id, value) => -value}.take(10)
    println("My PageRank", myPageRank.foreach(print))
    println("Spark PageRank", sparkPageRank.foreach(print))
  }

  def loadGraph(filename: String): Graph[Int, Int] =
    GraphLoader.edgeListFile(sparkContext, Paths.get(getClass.getResource(filename).toURI).toString, true)

  def pageRank(graph: Graph[Int, Int], converge: Double = 0.00001, dampingFactor: Double = 0.85): Graph[Double, Int] = {

    val outDegree = graph.outDegrees.collectAsMap()
    val const = (1 - dampingFactor) / graph.numVertices

    @tailrec
    def pageRankIter(graph: Graph[Double, Int]): Graph[Double, Int] = {
      val neighbor = graph.collectNeighbors(EdgeDirection.In).mapValues {
        x => x.foldLeft(0.0)((sum, p) => (sum + p._2 / outDegree(p._1)))
      }.collectAsMap()
      val newGraph = graph.mapVertices((id, p) => (const + dampingFactor * neighbor(id)))
      if (distance(newGraph, graph) < converge)
        newGraph
      else
        pageRankIter(newGraph)
    }

    def distance(g1: Graph[Double, Int], g2: Graph[Double, Int]): Double = {
      g1.vertices.join(g2.vertices).map { case (id, x) => math.abs(x._1 - x._2) }.sum
    }

    val numVertices = graph.numVertices
    pageRankIter(graph.mapVertices((id, attr) => (1.0 / numVertices)))
  }

}