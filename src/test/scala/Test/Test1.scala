package Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SPARK_VERSION, SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
  * Created by fanqingbo on 2017/1/3.
  */
//case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)

object Test1 {
  def main(args: Array[String]): Unit = {

    //    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //
    //    val conf = new SparkConf().setAppName("GraphXExample").setMaster("local")
    //    val sc = new SparkContext(conf)
    //    val sqlContext = new SQLContext(sc)
    //    case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)
    //    import sqlContext.implicits._
    //
    //    val bankText = sc.textFile("/test/bank-full.csv")
    //    val bank = bankText.map(s => s.split(";")).filter(s => s(0) != "\"age\"")
    //      .map(s => Bank(s(0).toInt,
    //        s(1).replaceAll("\"", ""),
    //        s(2).replaceAll("\"", ""),
    //        s(3).replaceAll("\"", ""),
    //s(5).replaceAll("\"", "").toInt
    //)
    //).toDF()
    //bank.registerTempTable("bank")
    //val df =sqlContext.sql("select age, count(1) from bank where age < 30 group by age order by age")
    //df.show()
    //    sc.stop()
    //
    //}

    print("hello world")
  }
}
