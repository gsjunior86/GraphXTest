package graphx

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

object GraphxTest {

  val friend_relation = "is-friends-with"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("testGraphx")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val nodes_rdd = sc.parallelize(Seq(
      (1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

    val edges_rdd = sc.parallelize(Seq(
      Edge(1L, 2L, friend_relation),
      Edge(2L, 3L, friend_relation),
      Edge(3L, 4L, friend_relation),
      Edge(3L, 5L, "wrote-status"),
      Edge(4L, 5L, "likes-status")))

    val graph = Graph(nodes_rdd, edges_rdd)

    graph.mapTriplets(x =>
      (x.attr, x.attr == friend_relation && x.srcAttr.toLowerCase().contains("a")))
      .triplets.foreach(println)

    val outDegree = graph.aggregateMessages[Int](
        (x =>x.sendToSrc(1))
        , _ + _)
      .rightOuterJoin(graph.vertices).map(x => (x._2._2, x._2._1.getOrElse(0))).collect()

    outDegree.foreach(println)

    def sendMsg(ec: EdgeContext[Int,String,Int]): Unit ={
      ec.sendToDst(ec.srcAttr + 1)
    }
    
    def mergeMsg(a: Int, b: Int):Int ={
      math.max(a, b)
    }
    
    def propagateEdgeCount(g: Graph[Int, String]): Graph[Int, String] = {
      val verts = g.aggregateMessages(sendMsg, mergeMsg)
      val g2 = Graph(verts,g.edges)
      val check = g2.vertices.join(g.vertices)
      .map(x => x._2._1 - x._2._2)
      .reduce(_ + _)
      
      if (check > 0)
        propagateEdgeCount(g2)
      else
        g
      
    }
    
    val initialGraph = graph.mapVertices((vId, str) => 0)
    propagateEdgeCount(initialGraph).vertices.collect().foreach(println)

  }

}