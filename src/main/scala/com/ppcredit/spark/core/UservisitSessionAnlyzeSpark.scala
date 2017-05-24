package com.ppcredit.spark.core

import org.apache.spark.{SparkConf, SparkContext,repl}

/**用户访问Session分析Spark作业
  * Created by fanqingbo on 2016/11/24.
  */

object UservisitSessionAnlyzeSpark {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("ad")
      .setMaster("local[2]")
    val str="adfffaf"
    val strr =str.mkString(",")
    println(strr)
//    Properties p =System.getProperties()
//    println(System.getProperties().get("hive-site.xml"))
val properties = System.getProperties()
    for ((k,v) <- properties) println(s"key: $k, value: $v")

//     import org.apache.spark.repl



    //    val sc = new SparkContext(sparkConf);
//
//    val sqlContext = new HiveContext(sc)


  }

}
