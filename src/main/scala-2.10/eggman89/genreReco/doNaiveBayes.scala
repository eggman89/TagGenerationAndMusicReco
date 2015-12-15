package eggman89.genreReco

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time
import org.joda.time.DateTime

/**
 * Created by snehasis on 12/5/2015.
 */
object doNaiveBayes {


  def train(trainset : RDD[LabeledPoint] , lamda: Int): NaiveBayesModel={

    val startTime =  new DateTime()
    println("Start: Training NaiveBayes with ", trainset.count(), " songs")
    val model = NaiveBayes.train(trainset, lambda = lamda, modelType = "multinomial")
    println("End: NaiveBayes Prediction")
    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime,endTime)
    println("Time to train:" , totalTime.toDuration.getStandardSeconds, "seconds")

    model
  }

  def test(model : NaiveBayesModel,testset : RDD[(Int,DenseVector,Int)] ): RDD[(Int, Int, Int)] = {
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
