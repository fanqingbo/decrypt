package com.ppcredit.spark.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, GraphLoader, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fanqingbo on 2016/12/30.
  */
object Test {
  def main(args: Array[String]): Unit = {


      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

      val conf = new SparkConf().setAppName("PageRank").setMaster("local")
      val sc = new SparkContext(conf)


    val friendsGraph = GraphLoader.edgeListFile(sc, "/user/fanqingbo/test.txt")

//    friendsGraph.vertices.collect.foreach((VertexID, Int))
//合并消息
    val totalRounds: Int = Int.MaxValue// total N round
    var targetVerticeID: Long = 4// target vertice// round one
    //初始化每个Vertice的属性为空map()集合，返回新的graph Step 2
    var roundGraph = friendsGraph.mapVertices((id, vd) => Map())

    //使用aggragateMessages 把VerticeID 和totalRounds传播出度点上，出度点把收集到的信息合成一个大的Map   Step3
    var roundVertices:VertexRDD[Map[Long, Integer]] = roundGraph.aggregateMessages[Map[Long, Integer]](
      ctx => {
        if (targetVerticeID == ctx.srcId) {
          // only the edge has target vertice should send msg
          //发送消息
          ctx.sendToDst(Map(ctx.srcId -> totalRounds))
        }
      },
      //合并消息，相同的被覆盖掉
      _ ++ _
    )
    println(roundVertices.collect.foreach(e=>println(e._1)))


    var roundVertices2:VertexRDD[Map[Long, Integer]] = roundGraph.aggregateMessages[Map[Long, Integer]](
      ctx => {
        if (targetVerticeID == ctx.srcId) {
          // only the edge has target vertice should send msg
          //发送消息
          ctx.sendToDst(Map(ctx.srcId -> totalRounds))
        }
      },
      //合并消息，相同的被覆盖掉
      _ ++ _
    )
    //找到2度联系人
    println(roundVertices2.collect.foreach(e=>println(e._1)))






//    println(roundVertices.collect.foreach(e=>println(e._2)))

//    println(roundVertices.collect.foreach(e=>println(e.toString())
    for (i <- 2 to totalRounds) {
      val thisRoundGraph = roundGraph.outerJoinVertices(roundVertices){ (vid, data, opt) => opt.getOrElse(Map[Long, Integer]()) }
      roundVertices = thisRoundGraph.aggregateMessages[Map[Long, Integer]](
        ctx => {
          val iterator = ctx.srcAttr.iterator
          while (iterator.hasNext) {
            val (k, v) = iterator.next
            if (v > 1) {
              val newV = v - 1
              ctx.sendToDst(Map(k -> newV))
              ctx.srcAttr.updated(k, newV)
            } else {
              // do output and remove this entry
            }
          }
        },
        (newAttr, oldAttr) => {
          if (oldAttr.contains(newAttr.head._1)) { // optimization to reduce msg
            oldAttr.updated(newAttr.head._1, 1) // stop sending this ever
          } else {
            oldAttr ++ newAttr
          }
        }
      )
    }
   //roundVertices.map(_._1).collect.foreach(v=>println(v))
  }



}
