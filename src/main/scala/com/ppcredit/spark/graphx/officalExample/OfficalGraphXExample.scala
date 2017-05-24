package com.ppcredit.spark.graphx.officalExample

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD



/**假设我想从一些文本文件构建图，将图限制为重要的关系和用户，在子图上运行页面排名，
  * 然后最终返回与最高用户相关联的属性。 我可以做到这一切只需几行GraphX
  * Created by fanqingbo on 2016/12/14.
  */
object OfficalGraphXExample {

  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val sparkConf = new SparkConf()
      .setAppName("GraphXOfficialExample")
      .setMaster("local[2]")
    val sc = new SparkContext(sparkConf);
    val users = (sc.textFile("/user/fanqingbo/users.txt")
      .map(line => line.split(",")).map( parts => (parts.head.toLong, parts.tail) ))


    // Parse the edge data which is already in userId -> userId format

    //将边加为graph
    val followerGraph = GraphLoader.edgeListFile(sc, "/user/fanqingbo/followers.txt")


//    followerGraph.vertices.collect.foreach(v=>println(v))
//    followerGraph.edges.collect.foreach(v=>println(v))




    // Attach the user attributes
    val graph = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      // Some users may not have attributes so we set them as empty
      case (uid, deg, None) => Array.empty[String]
    }

//    graph.vertices.collect.foreach(v=>println(v))
//    graph.edges.collect.foreach(v=>println(v))
    // Restrict the graph to users with usernames and names
    val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)



    // Compute the PageRank
    val pagerankGraph = subgraph.pageRank(0.001)

    // Get the attributes of the top pagerank users
    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }
    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))

  }
}
