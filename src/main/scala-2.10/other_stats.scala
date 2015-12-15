import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.Statistics._
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.rdd.RDD

/**
 * Created by sneha on 12/7/2015.
 */
class hashmap  extends java.io.Serializable
{
  var obj:Map[String,Int] = Map()
  var id = 0
  def add(value:String): Int ={

    if (obj.contains(value) == true)
    {
      obj(value)
    }

    else
    {
      id = id + 1
      obj = obj +(value->id)
      id
    }
  }

  def findval(value : Int) : String = {
    val default = ("-1",0)
    obj.find(_._2==value).getOrElse(default)._1
  }
}

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
