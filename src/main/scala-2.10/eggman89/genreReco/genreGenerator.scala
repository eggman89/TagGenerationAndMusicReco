package eggman89.genreReco

import eggman89.musicReco.hashmap
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.Map

object genreGenerator {
 def main(args: Array[String])  {
    //remove logging from console
    Logger.getLogger("org").setLevel(Level.OFF)
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

    val map_tagid_tag = new hashmap()
   sc.parallelize(map_tagid_tag.obj.toSeq).toDF("user", "userid").registerTempTable("userid")
   //map_tagid_tag
    val schema_string = "track_id1 tag_id"
    val schema = StructType(schema_string.split(" ").map(fieldName =>
      if (fieldName == "tag_id") StructField(fieldName, IntegerType, true)
      else StructField(fieldName, StringType, true))
    )


   val tag_id_hashhmap = new hashmap()
   val track_id_hashhmap = new hashmap()

   val RDD_song_details = sc.textFile("Dataset/msd_genre_dataset.txt").map(_.split(",")).map(p =>(tag_id_hashhmap.add(p(0)), track_id_hashhmap.add(p(1))

  // )
     //loudness,tempo,time_signature,key,mode,duration
     ,Vectors.dense(math.round(p(4).toString.toDouble).toDouble,math.round(p(5).toString.toDouble).toDouble,
     math.round(p(6).toString.toDouble),math.round(p(7).toString.toDouble)
     ,math.round(p(8).toString.toDouble),math.round(p(9).toString.toDouble)
     //avg timbre and var timbre : 12
     ,math.round(p(9).toString.toDouble),math.round(p(20).toString.toDouble)
     ,math.round(p(10).toString.toDouble),math.round(p(21).toString.toDouble)
     ,math.round(p(11).toString.toDouble),math.round(p(22).toString.toDouble)
     ,math.round(p(12).toString.toDouble),math.round(p(23).toString.toDouble)
     ,math.round(p(13).toString.toDouble),math.round(p(24).toString.toDouble)
     ,math.round(p(14).toString.toDouble),math.round(p(25).toString.toDouble)
     ,math.round(p(15).toString.toDouble),math.round(p(26).toString.toDouble)
     ,math.round(p(16).toString.toDouble),math.round(p(28).toString.toDouble)
     ,math.round(p(17).toString.toDouble),math.round(p(29).toString.toDouble)
     ,math.round(p(18).toString.toDouble),math.round(p(30).toString.toDouble)
     ,math.round(p(19).toString.toDouble),math.round(p(31).toString.toDouble)


   )
     )

   )
   //RDD_song_details.foreach(println)
    val split = RDD_song_details.randomSplit(Array(0.9,0.1))
    val RDD_train_tid_attributes_tag_id = split(0)
    val RDD_test_tid_attributes_tag_id = split(1)


   val RDD_LP_trainset: RDD[LabeledPoint] = RDD_train_tid_attributes_tag_id.map { line =>
     LabeledPoint(line._1, line._3)

   }

   val RDD_LP_testset: RDD[(Int,DenseVector,Int)]  = RDD_test_tid_attributes_tag_id.map { line => (line._2.toInt,
     line._3.toDense, line._1) }

   var predicted_res_RDD  : RDD[(Int, Int, Int)] = sc.emptyRDD

    //RDD_LP_tid_attributes_tag_id.take(500).foreach(println)
    if (method == 1)
      {
        predicted_res_RDD = doRandomForest.test(doRandomForest.train(RDD_LP_trainset,10,32,40),RDD_LP_testset)
      }

    if(method ==2)
      {
        predicted_res_RDD = doLogisticRegressionWithLBFGS.test(doLogisticRegressionWithLBFGS.train(RDD_LP_trainset),RDD_LP_testset)
      }

    if(method ==3)
    {
      predicted_res_RDD = doDecisionTrees.test(doDecisionTrees.train(RDD_LP_trainset,29,32),RDD_LP_testset)
    }

    if(method ==4)
    {
      predicted_res_RDD = doNaiveBayes.test(doNaiveBayes.train(RDD_LP_trainset,1),RDD_LP_testset)
    }
    if(method ==5)
      {
        chiSqTest.do_test(RDD_LP_trainset)

      }


   //calculate accuracy

    val predictionAndLabels : RDD[(Double,Double)] = predicted_res_RDD.toDF().map(l => (l(1).toString.toDouble,l(2).toString.toDouble))
    val metrics = new MulticlassMetrics(predictionAndLabels.coalesce(1))
    val precision = metrics.precision

    println("Precision = " + precision)
    println("End: Prediction")

   val RDD_orig = predicted_res_RDD.collect().map(p=>(track_id_hashhmap.findval(p._2),tag_id_hashhmap.findval(p._1),tag_id_hashhmap.findval(p._3)))
   RDD_orig.foreach(println)
  }

}

