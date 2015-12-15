package eggman89.genreReco

import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time
import org.joda.time.DateTime


/**
 * Created by snehasis on 12/5/2015.
 */
object doRandomForest {
  def train(trainset : RDD[LabeledPoint],maxD : Int, maxB:Int, numT:Int ): RandomForestModel =
  {
    val startTime =  new DateTime()
    println("Start: Training Random Forest with ", trainset.count(), " songs")
    val numClasses = 10
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = numT // Use more in practice.
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val impurity = "variance"
    val maxDepth = maxD
    val maxBins = maxB


    val model = RandomForest.trainRegressor(trainset, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    println("End: Random Forest Prediction")
    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime,endTime)
    println("Time to train:" , totalTime.toDuration.getStandardSeconds, "seconds")

    model
  }

  def test(model : RandomForestModel,testset : RDD[(Int,DenseVector,Int)] ): RDD[(Int, Int, Int)] = {
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
