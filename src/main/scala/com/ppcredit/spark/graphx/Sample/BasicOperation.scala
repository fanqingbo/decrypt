package com.ppcredit.spark.graphx.Sample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fanqingbo on 2016/12/26.
  */
object BasicOperation {

  def main(args: Array[String]) {
    //屏蔽日志
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
    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    //构造图Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    //***********************************************************************************
    //图的属性 操作，不改变图的结构，有vertices,edges,triplets,degree，filter等操作，适合select
    //**********************************************************************************
    /**
      * val vertices: VertexRDD[VD]
      * "点操作函数,对点属性的操作")
      */
    println("找出图中年龄大于30的顶点：")
    graph.vertices.filter { case (id, (name, age)) => age > 30}.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }
    /**
      *  val edges: EdgeRDD[ED]
      *  边操作函数，和filter函数使用的次数最多
      *edges操作：（srcId,dstId,attr)
      */
    println("找出图中属性大于5的边：")
    graph.edges.filter(e => e.attr > 5).collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println
    /**
      *   val triplets: RDD[EdgeTriplet[VD, ED]]
      *   triplets操作，和filter函数使用的次数最多
      *   triplets操作，((srcId, srcAttr), (dstId, dstAttr), attr)
      */
    println("列出边属性>5的tripltes：")
    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }

    println("找出图中最大的出度、入度、度数：")

    /**
      * 定义函数，比较大小
      * @param a
      * @param b
      * @return
      */
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    /**
      * lazy val degrees: VertexRDD[Int] =degreesRDD(EdgeDirection.Either).setName("GraphOps.degrees")
      * lazy val outDegrees: VertexRDD[Int] =degreesRDD(EdgeDirection.Out).setName("GraphOps.outDegrees")
      * lazy val inDegrees: VertexRDD[Int] =degreesRDD(EdgeDirection.In).setName("GraphOps.inDegrees")
      * 度数，出度，入度的操作
      */
    println("max of outDegrees:" + graph.outDegrees.reduce(max) + " max of inDegrees:" + graph.inDegrees.reduce(max) + " max of Degrees:" + graph.degrees.reduce(max))
    println

    //***********************************************************************************
    //转换操作  可以改变顶点或者边的属性  有mapVertices，mapEdges，mapTriplets等操作，适合update
    //**********************************************************************************
    /**def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2)  //属性的更改
         (implicit eq: VD =:= VD2 = null): Graph[VD2, ED]
          对点属性的更改函数
      */
    println("顶点的转换操作，顶点age + 10：")

    graph.mapVertices{ case (id, (name, age)) => (id, (name, age+10))}.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    /**  def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): Graph[VD, ED2] = {
            mapEdges((pid, iter) => iter.map(map))
      *对边属性更改的函数
      */
    println("边的转换操作，边的属性*2：")
    graph.mapEdges(e=>e.attr*2).edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println

    //***********************************************************************************
    // 结构操作 主要是构建子图上面应用，有reverse，subgraph，mask，groupEdges等函数，适合create
    //**********************************************************************************
    /** def subgraph(
      *
      epred: EdgeTriplet[VD, ED] => Boolean = (x => true), //边过滤函数
      vpred: (VertexId, VD) => Boolean = ((v, d) => true)) //点过滤函数
    : Graph[VD, ED]
      *Boolean分别是对应着边和点的filter函数
      */
    println("**********************************************************")
    println("结构操作")
    println("**********************************************************")
    println("顶点年纪>30的子图：")
    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)
    println("子图所有顶点：")
    subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    println
    println("子图所有边：")
    subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println


    //***********************************************************************************
    // 连接操作    也是对边或点的属性进行更改的操作  有joinVertices，outerJoinVertices等函数 适合update
    //**********************************************************************************
    /**def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])        //外部的vertex RDD
      (mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null)  //操作的函数
    : Graph[VD2, ED]
      和外部的vertex进行join，对点属性进行操作
      */
    println("**********************************************************")
    println("连接操作")
    println("**********************************************************")
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

    //创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
    val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0)}
    //initialUserGraph与inDegrees、outDegrees（RDD）进行连接，并修改initialUserGraph中inDeg值、outDeg值

    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg,outDegOpt.getOrElse(0))
    }
    println("连接图的属性：")
    userGraph.vertices.collect.foreach(v => println(s"${v._2.name} inDeg: ${v._2.inDeg}  outDeg: ${v._2.outDeg}"))
    println("出度和入读相同的人员：")
    userGraph.vertices.filter {
      case (id, u) => u.inDeg == u.outDeg
    }.collect.foreach {
      case (id, property) => println(property.name)
    }
    println

    //***********************************************************************************
    //聚合操作   主要是aggregateMessages函数 ****************************************
    //**********************************************************************************
    /**  def aggregateMessages[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,            //发送消息的函数
      mergeMsg: (A, A) => A,                               //聚合函数
      tripletFields: TripletFields = TripletFields.All)  //可选项，发送的方向
    : VertexRDD[A] = {
    aggregateMessagesWithActiveSet(sendMsg, mergeMsg, tripletFields, None)
  }
      *
      */
    println("**********************************************************")
    println("聚合操作")
    println("**********************************************************")
    println("找出年纪最大的追求者：")
    val oldestFollower: VertexRDD[(String, Int)] = userGraph.aggregateMessages[(String, Int)](
      // 将源顶点的属性发送给目标顶点，map过程
      triplet => {
        triplet.sendToDst(triplet.srcAttr.name, triplet.srcAttr.age)
      },
      // 得到最大追求者，reduce过程
      (a, b) => if (a._2 > b._2) a else b
    )
    userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
      optOldestFollower match {
        case None => s"${user.name} does not have any followers."
        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
      }
    }.collect.foreach { case (id, str) => println(str)}
    println
    sc.stop()
  }
}

