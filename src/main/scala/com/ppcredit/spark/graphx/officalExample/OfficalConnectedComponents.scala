package com.ppcredit.spark.graphx.officalExample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fanqingbo on 2016/12/28.
  *
  * 计算每个顶点的连接组件成员资格，并返回包含该顶点的连接组件中包含最低顶点id的顶点值的图。
  *
  * 连通算法目前不知道在社交场合是干什么用的，可能是级别划分、资格、权限 ，限制等
  *
  */
object OfficalConnectedComponents {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")
    val sc = new SparkContext(conf)


    val graph = GraphLoader.edgeListFile(sc, "/user/fanqingbo/followers.txt")
    // Find the connected components
    val cc = graph.connectedComponents().vertices

    println("----------------------------------------")
    // Join the connected components with the usernames
    val users = sc.textFile("/user/fanqingbo/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
//    def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = self.withScope {
//      join(other, defaultPartitioner(self, other))
//    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    println(ccByUsername.collect().mkString("\n"))
  }

}
