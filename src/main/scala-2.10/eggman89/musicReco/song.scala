package eggman89.musicReco

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.immutable.ListMap


object song {


  def get_similar(sqlContext: SQLContext, user_history: Map[String, Int]): Map[String, Int] = {

    var index = 0;    // to track the index in loop
    var i = 0;
    var song_tmp = ""
    var sim_songs = ""
    var parent_key = ""
    var map_sim_songs = Map[String, Int]() //Map to keep track of similar songs with priority


    var tid = ""
    var tidlist = ""
    var tidinorder = ""


    for (tid <- user_history.keysIterator) {
      tidlist = tidlist + "'" + tid + "', "
      tidinorder = tidinorder + " WHEN '" + tid + "' THEN " + (i + 1)
      i = i + 1
    }
    tidlist = tidlist.dropRight(2)

    //var sim_songs_list = df_similar.filter(df_similar("tid") === track_id).select("target").first().toString() // lists all the similar songs

    var sim_songs_df = sqlContext.sql("SELECT * FROM similar_table WHERE tid IN (" + tidlist + ")")

    for (sim_songs_list <- sim_songs_df.collect()) {

      //parsing and stuff

      parent_key = sim_songs_list.toString().substring(1,19)
      sim_songs = sim_songs_list.toString()
      sim_songs = sim_songs.dropRight(1)
      sim_songs = sim_songs.drop(20)

      var ar_similar_songs = sim_songs.split(",") //spliting the result by a comma
      index = 0 //initializing to 0 before loop
      //loop to put the similar songs and likness factor into a SortedMap
      for (song_tmp <- ar_similar_songs) {

        if (index % 2 == 0) // store the tid & frequency {
        {

          map_sim_songs += (ar_similar_songs(index).toString() -> (user_history(parent_key) * ar_similar_songs(index + 1).toString.toFloat.toInt))
        }
        index = index + 1
      }
    }


    var topuserscore = map_sim_songs.toSeq.sortWith (_._2 > _._2).head._2.toDouble
    topuserscore = topuserscore/10.0
    index = 0
    var map_sim_songs2 = Map[String, Int]()
    for (song_tmp <- map_sim_songs) {
      index = index + 1
      map_sim_songs2 += (song_tmp._1 -> math.round(song_tmp._2.toDouble/topuserscore).toInt)
    }

    new ListMap() ++ map_sim_songs2.toSeq.sortWith (_._2 > _._2).toSeq
  }

  def getDetails(sqlContext: SQLContext, track_id:String) :DataFrame=
  {
    sqlContext.sql("SELECT * FROM meta_table WHERE track_id = '" + track_id + "'")
  }

  def getDetails(sqlContext: SQLContext, id:Iterator[String]) :DataFrame=  {
    var tid=""
    var tidlist=""
    var tidinorder=""
    var index=0

    for (tid<-id)
    {
      // if(index < 20){
      tidlist = tidlist + "'" + tid + "', "
      tidinorder = tidinorder + " WHEN '" + tid+ "' THEN " + (index + 1)

      // }
      index = index + 1

    }
    tidlist = tidlist.dropRight(2)
    var sqlQuery = ""
    sqlQuery = "SELECT * FROM meta_table WHERE track_id IN (" + tidlist + ") ORDER BY CASE track_id" + tidinorder + " END"
    sqlContext.sql(sqlQuery)
  }

  def FinalResult(sc: SparkContext, sqlContext: SQLContext, user_history:Map[String, Int]) :DataFrame=
  {
    var sqlQuery = ""
    val schemaString = "track_id reco_conf"
    //val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val schema = StructType(
                  schemaString.split(" ").map(fieldName =>
                  if(fieldName == "track_id" ) StructField(fieldName, StringType, true)
                  else StructField(fieldName, FloatType, true))
    )
    val rowRDD = sc.parallelize(user_history.toSeq).map(_.toString().drop(1).dropRight(1).split(",")).map(x => Row(x(0), x(1).toFloat))

    val likeDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    likeDataFrame.registerTempTable("like_table")

    var songidlist=""

    for (songid<-user_history.keysIterator)      {
      //println("1:" +songid )
      songidlist =  songidlist + "'" + songid.toString().drop(1).dropRight(1) + "'" + ","
    }


    sqlQuery = "SELECT meta_table.track_id, title,artist_name,release,duration,year,reco_conf FROM meta_table RIGHT OUTER JOIN like_table ON like_table.track_id = meta_table.track_id ORDER BY like_table.reco_conf DESC "
    sqlContext.sql(sqlQuery)
  }

  def getAttributes(sqlContext: SQLContext, id:Iterator[String]) :DataFrame=
  {
    var tid=""
    var tidlist=""
    var tidinorder=""
    var index=0

    for (tid<-id)
    {

      tidlist = tidlist + "'" + tid + "', "
      tidinorder = tidinorder + " WHEN '" + tid+ "' THEN " + (index + 1)

      index = index + 1

    }
    tidlist = tidlist.dropRight(2)
    var sqlQuery = ""
    sqlQuery = "SELECT * FROM attributes WHERE track_id IN (" + tidlist + ") ORDER BY CASE track_id" + tidinorder + " END"
    sqlContext.sql(sqlQuery)
  }

}
