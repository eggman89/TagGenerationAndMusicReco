package eggman89.extra

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{VectorIndexer, StringIndexer}
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.classification._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.util.MLUtils


/**
  * Created by sneha on 12/16/2015.
  */
object test {

  def main(args:Array[String])
  {
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


    //load tags and tag ids and attributes

    val map_tagid_tag = new eggman89.hashmap()
    sc.parallelize(map_tagid_tag.obj.toSeq).toDF("user", "userid").registerTempTable("userid")
    //map_tagid_tag
    val schema_string = "track_id1 tag_id"
    val schema = StructType(schema_string.split(" ").map(fieldName =>
      if (fieldName == "tag_id") StructField(fieldName, IntegerType, true)
      else StructField(fieldName, StringType, true))
    )

    val DF_song_details = sc.textFile("C:/Users/sneha/Google Drive/Project/Dataset/msd_genre_dataset.txt").map(_.split(",")).map(p =>(p(0),p(1)



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

    ).toDF("track_id","genre","features")

    val labelIndexer = new StringIndexer()
      .setInputCol("genre")
      .setOutputCol("indexedLabel")
      .fit(DF_song_details)

    val Array(trainingData, testData) = DF_song_details.randomSplit(Array(0.7, 0.3))
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)
    // Train a GBT model.
    val rfc = new RandomForestClassifier().setMaxDepth(30).setNumTrees(20).
      setLabelCol("indexedLabel")
      .setFeaturesCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, rfc, labelConverter))

    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.show()
    predictions.select("track_id","predictedLabel","genre", "features","probability").show(5)

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]


  }

}
