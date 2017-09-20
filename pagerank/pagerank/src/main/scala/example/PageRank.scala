package example

import java.nio.file.Paths

import scala.annotation.tailrec

import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object PageRank {

  val spark = SparkSession.builder.appName("Page Rank").master("local").getOrCreate()
  val sparkContext = spark.sparkContext

  def main(args: Array[String]): Unit = {
    println("Page Rank", args(0))

    val originalGraph = loadGraph(args(0))
    println("# vertices: " + originalGraph.numVertices)
    println("# edges: " + originalGraph.numEdges)

    val sparkPageRankStart = System.currentTimeMillis()
    val sparkPageRank = originalGraph.pageRank(0.000001).vertices.sortBy { case (id, value) => -value }.take(10)
    val sparkPageRankTime = System.currentTimeMillis() - sparkPageRankStart
//
    val myPageRankStart = System.currentTimeMillis()
    val myPageRank = pageRank(originalGraph).vertices.sortBy { case (id, value) => -value }.take(10)
    val myPageRankTime = System.currentTimeMillis() - myPageRankStart

    val myPageRankPregelStart = System.currentTimeMillis()
    val myPageRankPregel = pageRankPregel(originalGraph).vertices.sortBy { case (id, value) => -value }.take(10)
    val myPageRankPregelTime = System.currentTimeMillis() - myPageRankPregelStart

    println("My PageRank", myPageRankTime)
    myPageRank.foreach(print)
    println("\nPageRank Pregel", myPageRankPregelTime)
    myPageRankPregel.foreach(print)
    println("\nSpark PageRank", sparkPageRankTime)
    sparkPageRank.foreach(print)
  }

  def loadGraph(filename: String): Graph[Int, Int] =
    GraphLoader.edgeListFile(sparkContext, Paths.get(getClass.getResource(filename).toURI).toString)

  def pageRank(graph: Graph[Int, Int], converge: Double = 0.000001, dampingFactor: Double = 0.85): Graph[Double, Int] = {

    val outDegree = graph.outDegrees.collectAsMap()
    val numVertices = graph.numVertices
    val const = (1 - dampingFactor) / numVertices
    val edges = graph.edges.cache

    @tailrec
    def pageRankIter(graph: Graph[Double, Int], iter: Int = 100): Graph[Double, Int] = {
      val neighbor = graph.collectNeighbors(EdgeDirection.In).mapValues {
        x => (x.foldLeft(0.0)((sum, p) => (sum + p._2 / outDegree(p._1))) * dampingFactor + const)
      }
      val newGraph = Graph(neighbor, edges).cache
      if (iter - 1 == 0 || distance(newGraph, graph) < converge)
        newGraph
      else{
        graph.unpersistVertices(false)
        pageRankIter(newGraph, iter - 1)
      }
    }

    def distance(g1: Graph[Double, Int], g2: Graph[Double, Int]): Double = {
//      g1.vertices.innerZipJoin(g2.vertices)((id, x, y) => math.abs(x - y)).map{case (id, x) => x}.sum
      g1.vertices.innerZipJoin(g2.vertices)((id, x, y) => math.abs(x - y)).aggregate(0.0)(_ + _._2, _ + _)
    }
    
    pageRankIter(graph.mapVertices((id, attr) => (1.0 / numVertices)))
  }


  def pageRankPregel(graph: Graph[Int, Int], converge: Double = 0.000001, dampingFactor: Double = 0.85): Graph[Double, Int] = {
    val outDegree = graph.outDegrees.collectAsMap()
    val numVertices = graph.numVertices
    val const = (1 - dampingFactor) / numVertices
    graph.mapVertices((id, attr) => 1.0).
    pregel(1.0 / numVertices, 100, EdgeDirection.Out)(
        (id, prob, newProb) => (newProb * dampingFactor + const), 
         x => Iterator((x.dstId, x.srcAttr / outDegree(x.srcId) )), 
        (x, y) => (x + y))
  }
}