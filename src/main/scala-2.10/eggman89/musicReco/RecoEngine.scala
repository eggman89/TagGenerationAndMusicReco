package eggman89.musicReco
import eggman89.hashmap
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


object RecoEngine {

  def main(args: Array[String]) {
    val path = "C:/Users/sneha/Google Drive/Project/Dataset/"
    val startTime = new DateTime()

    /*remove logging from console*/

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutil/")
    val conf = new SparkConf().setAppName("MusicReco").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.executor.memory","4g").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /*setting up sql context to query the data later on*/
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    println("Spark Context started")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)

    val userid_hashmap = new hashmap()
    val songid_hashmap = new hashmap()


    /*loading stuff into memory*/
   // println("Loading databases in memory ...")


    val rawUserSongPlaycount = sc.textFile(path + "train_triplets.txt").map(_.split("\t")).collect().map(p => Rating(userid_hashmap.add(p(0)), songid_hashmap.add(p(1)), p(2).toInt))

    val df_song_details = sqlContext.load("com.databricks.spark.csv", Map("path" -> (path + "SongDetails"), "header" -> "true"))

    val df_metadata = df_song_details.select("track_id","title","song_id","release", "artist_id","artist_name","duration","artist_familiarity","artist_hotttnesss","year")

    val df_attributes = df_song_details.select("track_id","song_id","danceability", "energy", "key", "loudness" , "tempo", "time_signature")
    val df_similar = sqlContext.load("com.databricks.spark.csv", Map("path" -> (path +"lastfm_similar_dest.csv"), "header" -> "true"))
    val df_new_attributes = df_song_details.where("year >= 2009").select("track_id", "danceability", "energy", "key", "loudness", "tempo", "time_signature")

    val text_train_triplets_all = sc.textFile(path + "train_triplets.txt")

    val schema_string = "user song_id play_count"
    val schema = StructType(schema_string.split(" ").map(fieldName =>
      if (fieldName == "play_count") StructField(fieldName, IntegerType, true)
      else StructField(fieldName, StringType, true))
    )
    val text_train_triplets_RDD = text_train_triplets_all.map(_.split("\t")).map(p => Row(p(0), p(1), p(2).toInt))

    val df_train_triplets_all = sqlContext.createDataFrame(text_train_triplets_RDD, schema)

    df_metadata.registerTempTable("meta_table")
    df_similar.registerTempTable("similar_table")
    df_attributes.registerTempTable("attributes")
   // df_train_triplets_all.registerTempTable("triplets_table")

    /*stats to blow your mind*/
    println("Total songs loaded :" + df_metadata.count() )
    println("Total users loaded :" + df_train_triplets_all.groupBy("user").agg($"user").count() )
    println("Total history loaded loaded :" + df_train_triplets_all.count() )
    println("Start Main program")

    val endTime = new DateTime()
    println("  ")

    /*main logic starts*/
    var user = 0
    while  (user != -1) {

    println("Input a user number (1-"+ df_train_triplets_all.groupBy("user").agg($"user").count() +"); -1 to exit the program ")
    user = readInt()

      //start : show user history
      val df_user_plays = df_train_triplets_all.where("user = '" + userid_hashmap.findval(user) + "'").sort($"play_count".desc)
      //val max_user_play =

      var songidlist=""

      for (songid<-df_user_plays.select("song_id").collect())      {
        songidlist =  songidlist + "'" + songid.toString().drop(1).dropRight(1) + "'" + ","
      }

      println("  ")
      println("User Profile Loaded for user number: " + user + "(" + userid_hashmap.findval(user) + ")")

      print("Top 10 songs listened by the user")
      val df_user_history = df_metadata.where("song_id IN (" +songidlist.dropRight(1) +")")
      val user_history_df = df_user_history.join(df_user_plays, df_user_history("song_id") === df_user_plays("song_id")).select("track_id", "title", "release", "artist_id","artist_name","duration" ,"artist_familiarity", "artist_hotttnesss","year","play_count"  ).where("year < 2009")
      user_history_df.select("title", "release","artist_name","duration" ,"year", "play_count" ).sort($"play_count".desc).show(10)

      /*end : show user history*/


      /*step 1 : Collaborative filtering */
      val trainData = sc.parallelize(rawUserSongPlaycount)

      println("Starting Collaborative filtering training")
      val model = ALS.trainImplicit(trainData, 10, 1, 0.01, 1.0)
      println("End:  Collaborative filtering training")
      var song_val: Map[String, Int] = Map()
      sc.parallelize(model.recommendProducts(1, 1000))

      println("Starting Collaborative filtering Recommendation")
      val recommendations = model.recommendProducts(user,10)
      var topcolab = recommendations.sortWith(_.rating > _.rating).head.rating
      topcolab = topcolab / 10
      println("End Collaborative filtering Recommendation")

      val song_val_temp = recommendations.flatMap {
        line => Some(songid_hashmap.obj.find(_._2 == line.product), math.round(line.rating / topcolab))
      }

      sc.parallelize(df_new_attributes.collect().toSeq)
      val temp_1 = sc.parallelize(song_val_temp)

      for (x <- temp_1.collect()) {
        song_val += (x._1.toString.drop(6).take(18) -> x._2.toInt) //list of similar songs by collabarative recco with weightage
        songidlist =  songidlist + "'" + x._1.toString.drop(6).take(18) + "'" + ","
      }

      val song_val_rdd = sc.parallelize(song_val.toSeq)
      val song_val_df = song_val_rdd.toDF("song_id1", "score")

      val df_reco_old_attributes1 = df_attributes.where("song_id IN (" + songidlist.dropRight(1) + ")")
      val df_reco_old_attributes = df_reco_old_attributes1.join(song_val_df , df_reco_old_attributes1("song_id") === song_val_df("song_id1") )

      /*step 2: find similar songs based on user recommended songs on last.fm*/
      val user_history_list = user_history_df.select("track_id", "play_count").map(r => Row(r(0), r(1)))

      //converting RDD to List
      var list_of_songs = Map[String, Int]()
      for (temp <- user_history_list.collect()) {
        var row = temp.toString().split(",")
        list_of_songs += (row(0).drop(1).toString -> row(1).dropRight(1).toInt)

      }

      val user_similar_songs = song.get_similar(sqlContext, list_of_songs) //Map for similar songs
      val user_similar_songs_DF = sc.parallelize(user_similar_songs.toSeq).toDF("track_id1","confidence")

      df_similar.unpersist()
      df_train_triplets_all.unpersist()

      /*step 3 : prepare training set*/

      var top_score = user_similar_songs_DF.select("confidence").first().toString().dropRight(1).drop(1).toDouble
      top_score = top_score / 10.00

      songidlist=""
      for (songid<-user_similar_songs_DF.select("track_id1").collect())      {
        songidlist =  songidlist + "'" + songid.toString().drop(1).dropRight(1) + "'" + ","
      }

      val user_similar_songs_attr1 = df_attributes.where("track_id IN (" + songidlist.dropRight(1) + ")")
      val user_similar_songs_attr = user_similar_songs_attr1.join(user_similar_songs_DF , user_similar_songs_attr1("track_id") === user_similar_songs_DF("track_id1") )

      val temp1 = user_similar_songs_attr.toDF("track_id", "song_id", "danceability", "energy", "key", "loudness", "tempo", "time_signature", "track_id1", "confidence")
        .select("track_id", "danceability", "energy", "key", "loudness", "tempo", "time_signature", "confidence").where("confidence > 0")
     // println(user_similar_songs_attr.count())
      val similar_songs_RDD_LP: RDD[LabeledPoint] = temp1.map(l =>
        if (l(1).toString.isEmpty == false & l(2).toString.isEmpty == false & l(4).toString.isEmpty == false & l(5).toString.isEmpty == false)

          LabeledPoint
          (math.round(l(6).toString.toDouble / top_score),
            Vectors.dense(math.round((l(1).toString.toDouble) * 10),
              math.round(l(2).toString.toDouble * 10),
              l(3).toString.toDouble,
              math.round(l(4).toString.toDouble),
              l(5).toString.toDouble))
        else LabeledPoint(0, Vectors.dense(0, 0, 0, 0, 0)))

      val temp2 = df_reco_old_attributes.toDF("track_id", "song_id", "danceability", "energy", "key", "loudness", "tempo", "time_signature", "track_id1", "confidence")
        .select("track_id", "danceability", "energy", "key", "loudness", "tempo", "time_signature", "confidence").where("confidence > 0")

      val final2 = temp2.select("track_id", "confidence").map(f => (f(0).toString, f(1).toString.toInt) )

      val colab_similar_songs_RDD_LP: RDD[LabeledPoint] = temp2.map(l =>
        if (l(1).toString.isEmpty == false & l(2).toString.isEmpty == false & l(4).toString.isEmpty == false)

          LabeledPoint
          (math.round(l(6).toString.toDouble),
            Vectors.dense(math.round((l(1).toString.toDouble) * 10),
              math.round(l(2).toString.toDouble * 10),
              l(3).toString.toDouble,
              math.round(l(4).toString.toDouble),
              l(5).toString.toDouble))
        else LabeledPoint(0, Vectors.dense(0, 0, 0, 0, 0)))


      val startTime = new DateTime()
      println("Start: Training LogisticRegressionWithLBFGS with ", 1000, " songs")
      val finalmodel = new LogisticRegressionWithLBFGS()
        .setNumClasses(11)
        .run(colab_similar_songs_RDD_LP)
        //.run(train_set)

      println("End: LogisticRegressionWithLBFGS Prediction")

      val temp3 = df_new_attributes.select("track_id", "danceability", "energy", "tempo", "key", "time_signature")


      val new_song_RDD: RDD[(String, Int, String)] =
        temp3.map(v =>
          if (v(1).toString.isEmpty == false & v(2).toString.isEmpty == false & v(4).toString.isEmpty == false & v(5).toString.isEmpty == false)
            ((v(0).toString,
              finalmodel.predict(Vectors.dense(math.round((v(1).toString.toDouble) * 10),
                math.round(v(2).toString.toDouble * 10),
                v(3).toString.toDouble,
                math.round(v(4).toString.toDouble),
                v(5).toString.toDouble)).toInt, "Hot"))
          else (v(0).toString, 0, "Hot"))

         songidlist=""

      /*merging all three sources*/
      var finallist1 : List[(String,Int,String)]  = Nil

      for (songid<-user_similar_songs)      {

        songidlist =  songidlist + "'" + songid._1.toString().dropRight(1).drop(1) + "'" + ","
        finallist1 = finallist1 ++  List((songid._1.toString(),songid._2,"."))
      }

      var finallist2 : List[(String,Int,String)]  = Nil
      for (songid<-final2.collect())      {

        songidlist =  songidlist + "'" + songid._1.toString() + "'" + ","
        finallist2 = finallist2 ++  List((songid._1.toString(),songid._2,".."))
      }

      var finallist3 : List[(String,Int,String)]  = Nil
      for (songid<-new_song_RDD.collect())
      {

        songidlist =  songidlist + "'" + songid._1.toString() + "'" + ","
        finallist3 = finallist3 ++  List((songid._1.toString(),songid._2,"Hot"))
      }

      val finallist4 =  finallist2 ++ finallist3
      val finallist5 = finallist1 ++finallist4

      println("Final recommendations")

      //prep final output
     val track_conf_RDD = sc.parallelize(finallist5.toSeq.map(x => (x._1,x._2,x._3) ))


     val track_conf_DF = track_conf_RDD.toDF("track_id1", "confidence", "HotorNot?").dropDuplicates()

    val final_meta = df_metadata.where("track_id IN (" +songidlist.dropRight(1) +")")

      final_meta.join(track_conf_DF, track_conf_DF("track_id1") === final_meta("track_id")).select("track_id","title", "artist_name", "release", "duration", "year", "confidence", "HotorNot?").sort($"confidence".desc).show(100)
    }
    sc.stop()
    println("Spark Context stopped")

  }
}
