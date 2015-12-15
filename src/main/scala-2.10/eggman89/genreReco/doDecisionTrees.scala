package eggman89.genreReco

import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time
import org.joda.time.DateTime

/**
 * Created by sneha on 12/5/2015.
 */
object doDecisionTrees {

  def train(trainset : RDD[LabeledPoint] , maxD : Int, maxB: Int ): DecisionTreeModel={

    val startTime =  new DateTime()
    val numClasses = 10
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = maxD
    val maxBins = maxB

    println("Start: Training DecisionTree with ", trainset.count(), " songs")
    val model = DecisionTree.trainClassifier(trainset, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    println("End: DecisionTree Prediction")
    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime,endTime)
    println("Time to train:" , totalTime.toDuration.getStandardSeconds, "seconds")
    model
  }
  def test(model : DecisionTreeModel,testset : RDD[(Int,DenseVector,Int)] ): RDD[(Int, Int, Int)] = {
    val startTime =  new DateTime()
    println("Start: Prediction of", testset.count(), "with DecisionTree ")

    val predicted_res_RDD:RDD[(Int, Int, Int)] = testset.map{line =>

      (line._1, model.predict(line._2).toInt,line._3)}


    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime,endTime)
    println("Time to test:" , totalTime.toDuration.getStandardSeconds, "seconds")

    predicted_res_RDD

  }
}
