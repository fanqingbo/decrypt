package com.ppcredit.spark.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by fanqingbo on 2016/12/29.
  */
object OfficalExample1 {

    def main(args: Array[String]): Unit = {
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
      val conf = new SparkConf().setAppName("PageRank").setMaster("local")
      val sc = new SparkContext(conf)
      val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
      val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
      val training = splits(0).cache()
      val test = splits(1)
      val numIterations = 100
      val model = SVMWithSGD.train(training, numIterations)
      model.clearThreshold()
      val scoreAndLabels = test.map { point =>
        val score = model.predict(point.features)
        (score, point.label)
      }

      // Get evaluation metrics.
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      val auROC = metrics.areaUnderROC()

      println("Area under ROC = " + auROC)

      // Save and load model
      model.save(sc, "myModelPath1")
      val sameModel = SVMModel.load(sc, "myModelPath1")
      sc.stop()
    }
}
