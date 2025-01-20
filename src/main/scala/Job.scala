import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.Commons

object Job {


  val path_to_datasets = "/datasets/processed/"

  val path_tracks = path_to_datasets + "tracks.csv"
  val path_playlists = path_to_datasets + "playlists.csv"
  val path_tracks_in_playlist = path_to_datasets + "tracks_in_playlist.csv"
  val path_artists = path_to_datasets + "artists.csv"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spotify job").getOrCreate()
    val sqlContext = spark.sqlContext // needed to save as CSV
    import sqlContext.implicits._

    //    if (args.length < 2) {
    //      println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
    //      println("The second parameter should indicate the job (1 for join-and-agg, 2 for agg-and-join, 3 for agg-and-bjoin)")
    //      return
    //    }

    val deploymentMode = args(0)
    var writeMode = deploymentMode
    if (deploymentMode == "sharedRemote") {
      writeMode = "remote"
    }
    //    val job = args(1)
    val job = "1"
    val rddTracks = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_tracks)).
      flatMap(CsvParser.parseTrackLine)

    val rddPlaylists = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_playlists)).
      flatMap(CsvParser.parsePlayListLine)

    val rddTracksInPlaylist = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_tracks_in_playlist)).
      flatMap(CsvParser.parseTrackInPlaylistLine)

    val rddArtists = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_artists)).
      flatMap(CsvParser.parseArtistLine)

    if (job == "1") {
      // For each artist, calculate the average number of songs in a playlist
      //      val rddTracksKV = rddTracks.map(x => (x.track_uri, x.artist_uri))
      //      val rddPlaylistsKV = rddPlaylists.map(x => (x.PID, 1))
      //      val rddArtistKV = rddArtists.map(x => (x.artist_uri, 1))
      //      val rddTracksInPlaylistKV = rddTracksInPlaylist.map(x => (x.track_uri, x.PID))
      val rddTracksKV = rddTracks.map(x => (x._1, x._4))
      val rddPlaylistsKV = rddPlaylists.map(x => (x._1, 1))
      val rddArtistKV = rddArtists.map(x => (x._1, 1))
      val rddTracksInPlaylistKV = rddTracksInPlaylist.map(x => (x._2, x._1))

      import spark.implicits._
      val rddTrackPlaylist = rddTracksInPlaylistKV.join(
        rddTracksKV
      )
      val rddTrackPlaylistArtist = rddTrackPlaylist.join(rddArtistKV)
      val artistSongCount =
        rddTrackPlaylistArtist
          .map(x => (x._2._2, x._2._1))
          .groupByKey()
          .mapValues(_.size)
          .collect()
      // save output as CSV
      val artistSongCountDF = artistSongCount.toSeq.toDF("artist_uri", "num_songs").write.format("csv").mode(SaveMode.Overwrite).save(Commons.getDatasetPath(writeMode, "/output/artist_song_count"))
      //      val avgSongOfEachArtistInAPlaylist = rddTracksInPlaylist.
      //        //        map(x => (x.track_uri, x.PID)).
      //        //        join(rddTracksKV).
      //        //        map(x => (x._2._2, x._2._1)).
      //        //        join(rddPlaylistsKV).
      //        //        map(x => (x._2._1, x._2._2)).
      //        //        reduceByKey(_ + _).
      //        //        join(rddArtistKV).
      //        //        map(x => (x._1, x._2._1.toDouble / x._2._2)).
      //        collect()

      // save output as CSV
    }
    else if (job == "2") {
      // Job Tommi
    }
  }

}
