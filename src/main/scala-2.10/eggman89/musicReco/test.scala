package eggman89.musicReco

import eggman89.musicReco.hashmap
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.joda.time
import org.joda.time.DateTime



object test {
  def main(args: Array[String]) {
    val path = "Dataset/"
    val startTime = new DateTime()

    //remove logging from console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutil/")
    val conf = new SparkConf().setAppName("MusicReco").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local")
    val sc = new SparkContext(conf)

    //setting up sql context to query the data later on
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    println("Spark Context started")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)

    val userid_hashmap = new hashmap()
    val songid_hashmap = new hashmap()

    val rawUserSongPlaycount = sc.textFile(path + "train_triplets.txt").map(_.split("\t")).collect().map(p => Rating(userid_hashmap.add(p(0)), songid_hashmap.add(p(1)), p(2).toInt))


      val trainData = sc.parallelize(rawUserSongPlaycount)

      println("Starting Collaborative filtering training")
      val model = ALS.trainImplicit(trainData, 10, 1, 0.01, 1.0)
      println("End:  Collaborative filtering training")
      var song_val: Map[String, Int] = Map()
      sc.parallelize(model.recommendProducts(1, 1000))

    var song_list : List[(Int, Int, Double)] = Nil

    for(user <- 1 to 10)
    {
      // song_list = ALSmodel.recommendProducts(user,10).map(p=>(p.user,user_hashmap.findval(p.product),p.rating)).toList
      song_list = song_list ++ model.recommendProducts(user,5).map(p=>(p.user,p.product,p.rating)).toList

    }

    //val test = sc.parallelize(song_list).toDF("user","song_id","rating").coalesce()
    //test.saveAsParquetFile()

    //sqlContext.sql("Select * from reccomendedsongs")

    sc.stop()
    println("Spark Context stopped")



  }




}
