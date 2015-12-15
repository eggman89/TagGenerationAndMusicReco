package eggman89.musicReco

import org.apache.spark.sql.{DataFrame, SQLContext}

object profile {
  def get_existing(sqlContext:SQLContext, profile_id:String): DataFrame=
  {

    val temp = sqlContext.sql("SELECT * FROM triplets_table WHERE user = '"  +profile_id+"'")
    //temp.show()
    temp.registerTempTable("temp1")
    val temp1 = sqlContext.sql("SELECT * FROM meta_table RIGHT OUTER JOIN temp1 ON  meta_table.song_id = temp1.song_id" )
    temp1.sort(temp1("play_count").desc)

  }

  def get_existing_with_attributes(sqlContext:SQLContext, profile_id:String): DataFrame={
    var profile_ext = profile.get_existing(sqlContext,profile_id).select("track_id","play_count").registerTempTable("attr")
    var sqlQuery = "SELECT * FROM attributes JOIN attr ON attributes.track_id = attr.track_id ORDER BY attr.play_count DESC"
    sqlContext.sql(sqlQuery)
  }

}
