package com.ppcredit.spark.graphx.Sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx._
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
object PregelBigData {
  
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local").setAppName("test1")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val inputPath = "/user/fanqingbo/graphx-wiki-edges.txt"
    val rawData = sc.textFile(inputPath).map { x => {
       val arr = x.split("	")
       (arr(0), arr(1))
    } }
    //身份证与id的映射
    val cards = rawData.flatMap(x => Array(x._1, x._2)).distinct()
    val cardsToIds = cards.zipWithUniqueId()
    val idsToCards = cardsToIds.map(x => (x._2, x._1))

    val tmpCardIds = rawData.leftOuterJoin(cardsToIds).map(x => {
      val card2 = x._2._1
      val id1 = x._2._2 match {
        case Some(a) => a
        case None => -1
      }
      (card2, id1)
    })
    val idRDD = tmpCardIds.leftOuterJoin(cardsToIds).map(x => {
      val id1 = x._2._1
      val id2 = x._2._2 match {
        case Some(a) => a
        case None => -1
      }
      (id1, id2)
    }).filter{x => {
      (x._1 != -1) && (x._2 != -1)
    }}
    
    
    //图的构建
    val edgeRDD = idRDD.map ( row => 
      (new Edge(row._1, row._2, 1L)))
    val graph = Graph.fromEdges(edgeRDD, None)
                .partitionBy(PartitionStrategy.EdgePartition2D)
                
                
    //一度邻居节点的收集
    val firstNeighbors = graph.ops.collectNeighborIds(EdgeDirection.Out).cache()
    val dgraph = graph.outerJoinVertices(firstNeighbors)((vid, u, successors) => {
      successors match{
        case Some(deg) => deg
        case None => Array.empty
      }
    })
    //二度关系的输出
    val secRDD = dgraph.triplets.flatMap(triplet =>{
      val srcId = triplet.srcId
      val arr = triplet.dstAttr.diff(triplet.srcAttr :+ srcId).map { x => (srcId.toLong, x.toLong) }
      arr
    }).distinct()
    
    val tmpIDCards = secRDD.leftOuterJoin(idsToCards).map(x => {
      val id2 = x._2._1
      val card1 = x._2._2 match {
        case Some(a) => a
        case None => "-1"
      }
      (id2, card1)
    })
    
    //二度关系的全体结果就是secCardRDD
    val secCardRDD = tmpIDCards.leftOuterJoin(idsToCards).map(x => {
      val card1 = x._2._1
      val card2 = x._2._2 match {
        case Some(a) => a
        case None => "-1"
      }
      (card1, card2)
    }).filter{x => {
      (x._1 != "-1") && (x._2 != "-1")
    }}

    val id = "168437400931144903"
    val secDegreesOfId = secCardRDD.filter{x => (x._1 == id || x._2 == id)}
    secDegreesOfId.foreach(println)
    sc.stop()
  }
}