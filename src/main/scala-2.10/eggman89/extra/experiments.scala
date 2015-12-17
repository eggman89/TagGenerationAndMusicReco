import breeze.numerics.abs
import eggman89.genreReco.{chiSqTest, doNaiveBayes, doDecisionTrees}
import eggman89.musicReco._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, Row, DataFrameStatFunctions}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import eggman89.musicReco
import org.joda.time
import org.joda.time.DateTime

/*object global{

  val artistid_hashmap = new eggman89.hashmap(sc)
}
object doLogisticRegressionWithLBFGS2 {


  def train(df_train_tid_attributes_tag_id: DataFrame,RDD_LP_tid_attributes_tag_id : RDD[LabeledPoint] ): LogisticRegressionModel=
  {

    println("Start: Training LogisticRegressionWithLBFGS2 with ", df_train_tid_attributes_tag_id.count(), " songs")
    val startTime =  new DateTime()
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(16)
      .run(RDD_LP_tid_attributes_tag_id)
    println("End: LogisticRegressionWithLBFGS2 Prediction")
    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime,endTime)
    println("Time to train:" , totalTime.toDuration.getStandardSeconds, "seconds")
    model

  }

  def test(model : LogisticRegressionModel,df_test_tid_attributes_tag_id : DataFrame ): RDD[(String, Int, String)] = {
    val startTime =  new DateTime()
    df_test_tid_attributes_tag_id.show()
    println("Start: Prediction of", df_test_tid_attributes_tag_id.count(), "with LogisticRegressionWithLBFGS ")
    val predicted_res_RDD:RDD[(String, Int, String)] = df_test_tid_attributes_tag_id.map(l =>
      if (l(1).toString.isEmpty == false & l(2).toString.isEmpty == false & l(3).toString.isEmpty == false & l(4).toString.isEmpty == false)
        ((l(0).toString,
          (model.predict(Vectors.dense(math.round((l(1).toString.toDouble) * 10),
            math.round(l(2).toString.toDouble * 10),
            l(3).toString.toDouble,
            math.round(l(4).toString.toInt.toDouble),

            math.round(l(5).toString.toDouble/10),
          global.artistid_hashmap.add(l(6).toString)
          )).toInt), l(7).toString))
      else (l(0).toString, 0, (l(7).toString.toInt).toString))


    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime,endTime)
    println("Time to test:" , totalTime.toDuration.getStandardSeconds, "seconds")
    predicted_res_RDD

  }
}
object tagGenerator2 {
  def main(args: Array[String]) {
    //remove logging from console
    /*  Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutil/")

    val conf = new SparkConf().setAppName("MusicReco").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //setting up sql context to query the data later on
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    println("Spark Context started")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)


    println("Select a Method to classify songs")
    println("1: Random Forest; 2:Logistic Regression With LBFGS; 3:Decision Trees;  4:Naive Bayes 5:chiSqTest(other)")
    val method = readInt()
    //load tags and tag ids and attributes
    val map_tagid_tag0 = new eggman89.hashmap()
    val map_tagid_tag = new eggman89.hashmap()
    val schema_string = "track_id1 tag_id"
    val schema = StructType(schema_string.split(" ").map(fieldName =>
      if (fieldName == "tag_id") StructField(fieldName, IntegerType, true)
      else StructField(fieldName, StringType, true))
    )

    val tid_trackid = sc.textFile("D:/Project/FinalDataset/track_id_to_tag.txt").map(_.split("\t")).map(p => Row(p(0), map_tagid_tag0.add(p(1))))
    val df_track_id_tag_id = sqlContext.createDataFrame(tid_trackid, schema)
    val temp = sc.textFile("D:/Project/FinalDataset/track_id_to_tag.txt").collect().map(_.split("\t")).map(p => Row(p(0), map_tagid_tag.add(p(1))))
    val df_metadata = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/track_metadata_without_dup.csv", "header" -> "true"))
    val df_attributes =  sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/song_attributes.csv", "header" -> "true"))

    //merge tid with song_attributed
    val df_with_artist_id =  df_metadata.select("track_id","artist_id").toDF("track_id2","artist_id")


    val df_tid_attributes = df_attributes.join(df_track_id_tag_id, df_track_id_tag_id("track_id1") ===df_attributes("track_id") ).select("track_id", "danceability","energy", "tempo" ,"key","time_signature")
    val df_tid_attributes_tag_id = df_tid_attributes.join(df_track_id_tag_id, df_track_id_tag_id("track_id1") ===df_tid_attributes("track_id"))
    val test = df_tid_attributes_tag_id.join(df_with_artist_id, df_with_artist_id("track_id2") === df_tid_attributes_tag_id("track_id"))
    val test1 = test.select("track_id", "danceability","energy", "tempo" ,"key","time_signature","artist_id","tag_id")
    //println(test1.count())
    val split = test1.randomSplit(Array(0.9, 0.1))
    val df_train_tid_attributes_tag_id = split(0)
    val df_test_tid_attributes_tag_id = split(1)
    //df_train_tid_attributes_tag_id.show()
    //df_test_tid_attributes_tag_id.show()
    val RDD_LP_tid_attributes_tag_id: RDD[LabeledPoint] = df_train_tid_attributes_tag_id.map(l =>
      if (l(1).toString.isEmpty == false & l(2).toString.isEmpty == false & l(3).toString.isEmpty == false & l(4).toString.isEmpty == false)
        (LabeledPoint
        ((l(7).toString.toInt) ,
          Vectors.dense(math.round((l(1).toString.toDouble)*10),
            math.round(l(2).toString.toDouble*10),
            l(3).toString.toDouble,
            math.round(l(4).toString.toDouble),
            (l(5).toString.toDouble/10).toInt,
            global.artistid_hashmap.add(l(6).toString)
          )))
      else
        LabeledPoint(0 , Vectors.dense(0.0,0.0,0.0,0,0,0)))


    //train and test
    var predicted_res_RDD: RDD[(String, Int, String)] = sc.emptyRDD

    //RDD_LP_tid_attributes_tag_id.take(500).foreach(println)
    if (method == 1)
    {
      predicted_res_RDD = doRandomForest.test(doRandomForest.train(df_train_tid_attributes_tag_id,RDD_LP_tid_attributes_tag_id),df_test_tid_attributes_tag_id)
    }

    if(method ==2)
    {
      predicted_res_RDD = doLogisticRegressionWithLBFGS2.test(doLogisticRegressionWithLBFGS2.train(df_train_tid_attributes_tag_id,RDD_LP_tid_attributes_tag_id),df_test_tid_attributes_tag_id)
    }

    if(method ==3)
    {
      predicted_res_RDD = doDecisionTrees.test(doDecisionTrees.train(df_train_tid_attributes_tag_id,RDD_LP_tid_attributes_tag_id),df_test_tid_attributes_tag_id)
    }

    if(method ==4)
    {
      predicted_res_RDD = doNaiveBayes.test(doNaiveBayes.train(df_train_tid_attributes_tag_id,RDD_LP_tid_attributes_tag_id),df_test_tid_attributes_tag_id)
    }
    if(method ==5)
    {
      chiSqTest.do_test(RDD_LP_tid_attributes_tag_id)
      return
    }


    //calculate accuracy
println(global.artistid_hashmap.id)
    // predicted_res_RDD.foreach(println)
    //predicted_res_RDD.foreach(println)
    val predictionAndLabels : RDD[(Double,Double)] = predicted_res_RDD.toDF().map(l => (l(1).toString.toDouble,l(2).toString.toDouble))
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)
    println("End: Prediction")
  }*/
  }
}*/

