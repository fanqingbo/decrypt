package com.ppcredit.spark.mllib
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD


/**
  * Created by fanqingbo on 2016/12/29.
  */
object OfficalExample {

    def main(args: Array[String]): Unit = {
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
      val conf = new SparkConf().setAppName("PageRank").setMaster("local")
      val sc = new SparkContext(conf)
//      val point = LabeledPoint.parse("(0.0, [1.0, 2.0])")
//
//      val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
//
//      val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
//
//      val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
//      println(dv == sv2)
//      println("----------------简要统计----------------")
//      val observations = sc.parallelize(
//          Seq(
//            Vectors.dense(1.0, 10.0, 100.0),
//            Vectors.dense(2.0, 20.0, 200.0),
//            Vectors.dense(3.0, 30.0, 300.0)
//          )
//      )
//
//      val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
//        //列平均数
//      println(summary.mean)
//        //列向量方差
//      println(summary.variance)
//        //列的非零个数
//      println(summary.numNonzeros)
//      println("-------------相关统计----------------------")
//      println("使用persion的方法计算向量")
//
//        val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5))  // a series
//
//        val seriesY: RDD[Double] = sc.parallelize(Array(11, 22, 33, 33, 555))
//
//        val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
//        println(correlation.toString)
//
//      println("使用persion的方法计算矩阵")
//      val data: RDD[Vector] = sc.parallelize(
//                Seq(
//                  Vectors.dense(1.0, 10.0, 100.0),
//                  Vectors.dense(2.0, 20.0, 200.0),
//                  Vectors.dense(5.0, 33.0, 366.0))
//              )
//            val correlMatrix: Matrix = Statistics.corr(data, "spearman")
//            println(correlMatrix.toString)

//      println("------------分层抽样----------")
//
//
//      val data = sc.parallelize(
//        Seq((1, 'a'), (1, 'b'), (2, 'c'), (2, 'd'), (2, 'e'), (3, 'f')))
//
//      // specify the exact fraction desired from each key
//      val fractions = Map(1 -> 0.1, 2 -> 0.6, 3 -> 0.3)
//
//      // Get an approximate sample from each stratum
//      val approxSample = data.sampleByKey(withReplacement = false, fractions = fractions)
//      // Get an exact sample from each stratum
//      val exactSample = data.sampleByKeyExact(withReplacement = false, fractions = fractions)

      println("------------假设检测----------")
      val vec: Vector = Vectors.dense(0.1, 0.15, 0.2, 0.3, 0.25)
      val goodnessOfFitTestResult = Statistics.chiSqTest(vec)
      println(goodnessOfFitTestResult)
//      println(s"$goodnessOfFitTestResult\n")
//      val mat: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
//      val independenceTestResult = Statistics.chiSqTest(mat)
//      println(s"$independenceTestResult\n")
//      val obs: RDD[LabeledPoint] =
//        sc.parallelize(
//          Seq(
//            LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0)),
//            LabeledPoint(1.0, Vectors.dense(1.0, 2.0, 0.0)),
//            LabeledPoint(-1.0, Vectors.dense(-1.0, 0.0, -0.5)
//            )
//          )
//        )
//      val featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(obs)
//      featureTestResults.zipWithIndex.foreach { case (k, v) =>
//        println("Column " + (v + 1).toString + ":")
//        println(k)
//      }
      sc.stop()
    }
}
