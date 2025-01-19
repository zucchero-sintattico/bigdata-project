import org.apache.spark.sql.SparkSession

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
    //val job = args(1)

    val rddTracks = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_tracks)).
      flatMap(CsvParser.parseTrackLine)
      .collect()

    val rddPlaylists = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_playlists)).
      flatMap(CsvParser.parsePlayListLine)

    val rddTracksInPlaylist = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_tracks_in_playlist)).
      flatMap(CsvParser.parseTrackInPlaylistLine)

    val rddArtists = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_artists)).
      flatMap(CsvParser.parseArtistLine)
  }

}
