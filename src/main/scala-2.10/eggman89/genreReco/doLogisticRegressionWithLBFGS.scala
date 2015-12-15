package eggman89.genreReco

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time
import org.joda.time.DateTime

/**
 * Created by sneha on 12/5/2015.
 */
object doLogisticRegressionWithLBFGS {


  def train(trainset : RDD[LabeledPoint]): LogisticRegressionModel=
  {

    println("Start: Training LogisticRegressionWithLBFGS with ", trainset.count(), " songs")
    val startTime =  new DateTime()
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(trainset)
    println("End: LogisticRegressionWithLBFGS Prediction")
    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime,endTime)
    println("Time to train:" , totalTime.toDuration.getStandardSeconds, "seconds")
    model

  }

  def test(model : LogisticRegressionModel,testset : RDD[(Int,DenseVector,Int)] ): RDD[(Int, Int, Int)] = {
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
