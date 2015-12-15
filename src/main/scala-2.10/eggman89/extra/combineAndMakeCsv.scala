package eggman89.extra

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime

/**
  * Created by snehasis on 12/14/2015.
  */
object combineAndMakeCsv {

  def main(args: Array[String]) {

    val path = "C:/Users/sneha/Google Drive/Project/Dataset/"
    val startTime = new DateTime()

    //remove logging from console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutil/")
    val conf = new SparkConf().setAppName("MusicReco").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.executor.memory","4g").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //setting up sql context to query the data later on
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    println("Spark Context started")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)

    val df_song_metadata = sqlContext.load("com.databricks.spark.csv", Map("path" -> (path + "track_metadata_without_dup.csv"), "header" -> "true"))
    val df_song_attributes = sqlContext.load("com.databricks.spark.csv", Map("path" -> (path + "song_attributes.csv"), "header" -> "true"))

    df_song_metadata.join(df_song_attributes, df_song_attributes("track_id" ) === df_song_metadata("track_id"))
      .toDF("track_id","title","song_id","release","artist_id","artist_mbid","artist_name","duration","artist_familiarity","artist_hotttnesss","year","track_7digitalid","shs_perf","shs_work","track_id1","danceability","energy","key","loudness","tempo","time_signature")
     .select("track_id","title","song_id","release","artist_id","artist_mbid","artist_name","duration","artist_familiarity","artist_hotttnesss","year","track_7digitalid","shs_perf","shs_work","danceability","energy","key","loudness","tempo","time_signature").write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("newcars.csv")
  }

}
