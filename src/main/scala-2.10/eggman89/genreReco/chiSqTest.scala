package eggman89.genreReco

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.rdd.RDD

/**
  * Created by snehasis on 12/14/2015.
  */
object chiSqTest {
  def do_test(attributes: RDD[LabeledPoint]) {
    val featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(attributes)
    var i = 1
    featureTestResults.foreach { result =>
      println(s"Column $i:\n$result")
      i += 1
    } // summary of the test

  }
}
