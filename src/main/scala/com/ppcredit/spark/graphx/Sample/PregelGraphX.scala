package com.ppcredit.spark.graphx.Sample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
  * Created by fanqingbo on 2017/1/3.
  */
object PregelGraphX {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("GraphXExample").setMaster("local")
    val sc = new SparkContext(conf)

    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String,Int)
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )


    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    //构造图Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    type VMap=Map[VertexId,Int]

    /**
      * 节点数据的更新 就是集合的union
      */
    def vprog(vid:VertexId,vdata:VMap,message:VMap)
    :Map[VertexId,Int]=addMaps(vdata,message)

    /**
      * 发送消息
      */
    def sendMsg(e:EdgeTriplet[VMap, _])={

        //取两个集合的差集  然后将生命值减1
        val srcMap=(e.dstAttr.keySet -- e.srcAttr.keySet).map { k => k->(e.dstAttr(k)-1) }.toMap
        val dstMap=(e.srcAttr.keySet -- e.dstAttr.keySet).map { k => k->(e.srcAttr(k)-1) }.toMap

      if(srcMap.size==0 && dstMap.size==0)
        Iterator.empty
      else
        Iterator((e.dstId,dstMap),(e.srcId,srcMap))
    }
    /**
      * 消息的合并
      */
    def addMaps(spmap1: VMap, spmap2: VMap): VMap =
      (spmap1.keySet ++ spmap2.keySet).map {
        k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
      }.toMap
    val two=3  //这里是二跳邻居 所以只需要定义为2即可
    val newG=graph.mapVertices((vid,_)=>Map[VertexId,Int](vid->two))
      .pregel(Map[VertexId,Int](), two, EdgeDirection.Out)(vprog, sendMsg, addMaps)

    val twoJumpFirends=newG.vertices
      .mapValues(_.filter(_._2==0).keys)

    //查询的id
    val id = 5;
    println("-------------------------------------------------------------------")
    println("查询"+id +"的"+two+ "度结果是")
    println("-------------------------------------------------------------------")
    twoJumpFirends.collect().foreach(
      n =>
        if(id == n._1)
          println(n._2.mkString(","))
    )
    sc.stop()
  }
}
