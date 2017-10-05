package example

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object PowerLaw {
  val spark = SparkSession.builder.appName("Page Rank").master("local").getOrCreate()
  val sparkContext = spark.sparkContext

  def main(args: Array[String]): Unit = {
    println("Hello Power Law: " + args(0))

    val originalGraph = loadGraph(args(0))
    
    val degree = originalGraph.degrees
    
    println(calculateCAndAlpha(originalGraph,1))
    

  }

  def loadGraph(filename: String): Graph[Int, Int] = {
    GraphLoader.edgeListFile(sparkContext, filename)
  }
  
  def printDegree(filename: String): Unit = {
    
  }
  
  def calculateCAndAlpha(graph: Graph[Int, Int], kMin: Int): (Double, Double) = {
    
    val alpha = 1
    val C = (alpha - 1) * math.pow(kMin, alpha - 1)
    
    return (C, alpha)
  }
}