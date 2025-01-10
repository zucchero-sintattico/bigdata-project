import java.io.{BufferedWriter, File, FileWriter, IOException}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SparkFileUtils

import java.nio.file.{FileSystems, Files}
import scala.collection.mutable
import scala.io.Source

object SpotifyParser {
  case class Track(uri: String, name: String, duration: Int, artistUri: String, albumUri: String, albumName: String)

  case class Playlist(pid: Int, name: String, numFollowers: Int)

  case class TrackInPlaylist(pid: Int, trackUri: String, pos: Int)

  case class Artist(uri: String, name: String)

  val path_to_datasets = "C:/Users/tbrin/Desktop/bigdata-project/datasets/spotify/data/"
  val path_to_output = "C:/Users/tbrin/Desktop/bigdata-project/datasets/output/"

  // write the output in csv
  def appendToCsv[T](data: Seq[T], csvFile: String, headers: Seq[String])(implicit toMap: T => Map[String, Any]): Unit = {
    val file = new File(csvFile)
    val outputDir = file.getParentFile
    if (!outputDir.exists() && outputDir != null) {
      outputDir.mkdirs()
    }
    val writer = new BufferedWriter(new FileWriter(file, true))
    try {
      if(!file.exists() || file.length() == 0) {
        writer.write(headers.mkString(",") + "\n")
      }
      data.foreach { row =>
        val line = headers.map(h => toMap(row).getOrElse(h, "").toString).mkString(",")
        writer.write(line + "\n")
      }
    } finally {
      writer.close()
    }
  }

  // implicit conversion to Map
  implicit val playlistToMap: Playlist => Map[String, Any] = p => Map(
    "pid" -> p.pid,
    "name" -> p.name,
    "numFollowers" -> p.numFollowers
  )
  implicit val trackToMap: Track => Map[String, Any] = t => Map(
    "uri" -> t.uri,
    "name" -> t.name,
    "duration" -> t.duration,
    "artistUri" -> t.artistUri,
    "albumUri" -> t.albumUri,
    "albumName" -> t.albumName
  )
  implicit val trackInPlaylistToMap: TrackInPlaylist => Map[String, Any] = t => Map(
    "pid" -> t.pid,
    "trackUri" -> t.trackUri,
    "pos" -> t.pos
  )
  implicit val artistToMap: Artist => Map[String, Any] = a => Map(
    "uri" -> a.uri,
    "name" -> a.name
  )

  def main(args: Array[String]): Unit = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val files = Files.list(FileSystems.getDefault.getPath(path_to_datasets)).toArray.map(_.toString).take(10)

    // path to the output files
    val playlistsCsv = path_to_output + "playlists.csv"
    val tracksCsv = path_to_output + "tracks.csv"
    val trackInPlaylistCsv = path_to_output + "track_in_playlist.csv"
    val artistsCsv = path_to_output + "artists.csv"

    var counter = 0

    files.foreach { file =>
      println(s"Processing file: ${counter}")
      //increment the counter
      counter += 1

      val jsonString = Source.fromFile(file).getLines().mkString
      val jsonData = mapper.readValue(jsonString, classOf[Map[String, Any]])
      val playlistsRaw = jsonData("playlists").asInstanceOf[List[Map[String, Any]]]

      // list for the data
      val playlists = mutable.ListBuffer[Playlist]()
      val tracks = mutable.ListBuffer[Track]()
      val trackInPlaylist = mutable.ListBuffer[TrackInPlaylist]()
      val artists = mutable.Set[Artist]()

      // Converte i dati delle playlist
      playlistsRaw.foreach { playlist =>
        playlists += Playlist(
          pid = playlist("pid").asInstanceOf[Int],
          name = playlist("name").toString,
          numFollowers = playlist("num_followers").asInstanceOf[Int]
        )

        // Converte i dati dei brani
        val tracksInPlaylist = playlist("tracks").asInstanceOf[List[Map[String, Any]]]
        tracksInPlaylist.zipWithIndex.foreach { case (track, index) =>
          tracks += Track(
            uri = track("track_uri").toString,
            name = track.getOrElse("track_name", "").toString,
            duration = track.getOrElse("duration_ms", 0).asInstanceOf[Int],
            artistUri = track("artist_uri").toString,
            albumUri = track.getOrElse("album_uri", "").toString,
            albumName = track.getOrElse("album_name", "").toString
          )

          trackInPlaylist += TrackInPlaylist(
            pid = playlist("pid").asInstanceOf[Int],
            trackUri = track("track_uri").toString,
            pos = index
          )

          artists += Artist(
            uri = track("artist_uri").toString,
            name = track("artist_name").toString
          )
        }
      }
      // write in csv
      appendToCsv(playlists.toSeq, playlistsCsv, Seq("pid", "name", "numFollowers"))
      appendToCsv(tracks.toSeq, tracksCsv, Seq("uri", "name", "duration", "artistUri", "albumUri", "albumName"))
      appendToCsv(trackInPlaylist.toSeq, trackInPlaylistCsv, Seq("pid", "trackUri", "pos"))
      appendToCsv(artists.toSeq, artistsCsv, Seq("uri", "name"))
    }

    val conf = new SparkConf()
      .setAppName("Preprocessing")
      .setMaster("local[*]")
      .set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") // Disabilita file _SUCCESS e .crc
      .set("spark.local.dir", "C:/Users/tbrin/Desktop/bigdata-project/temp") // Directory temporanea
      .set("spark.cleaner.referenceTracking", "false") // Disabilita lo shutdown hook
      .set("spark.worker.cleanup.enabled", "false") // Disabilita il cleanup
      .set("spark.worker.cleanup.interval", "3600000") // Imposta un intervallo lungo
      .set("spark.files.overwrite", "true") // Sovrascrive i file senza errore

    val spark = SparkSession.builder()
      .appName("Preprocessing")
      .master("local[*]")
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    // artists
    def parseArtist(row: String): (String, String) = {
      val fields = row.split(",")
      val uri = fields(0)
      val name = fields(1)
      (uri, name)
    }
    val rddArtists = sc.textFile(artistsCsv).map(x => parseArtist(x)).distinct()

    val dfArtists = rddArtists.toDF("uri", "name")
    dfArtists.coalesce(1).write.option("header", "true").csv(path_to_output + "temp_cleaned_artists.csv")

    // playlists
    def parsePlaylist(row: String): (String, String, String) = {
      val fields = row.split(",")
      val pid = fields(0)
      val name = fields(1)
      val numFollowers = fields(2)
      (pid, name, numFollowers)
    }
    val rddPlaylist = sc.textFile(playlistsCsv).map(x => parsePlaylist(x)).distinct()
    rddPlaylist.toDF("pid", "name", "numFollowers").coalesce(1).write.option("header", "true").csv(path_to_output + "temp_cleaned_playlists.csv")

    // tracks
    def parseTrack(row: String): (String, String, String, String, String, String) = {
      val fields = row.split(",")
      val uri = fields(0)
      val name = fields(1)
      val duration = fields(2)
      val artistUri = fields(3)
      val albumUri = fields(4)
      val albumName = fields(5)
      (uri, name, duration, artistUri, albumUri, albumName)
    }
    val rddTracks = sc.textFile(tracksCsv).map(x => parseTrack(x)).distinct()
    rddTracks.toDF("uri", "name", "duration", "artistUri", "albumUri", "albumName").coalesce(1).write.option("header", "true").csv(path_to_output + "temp_cleaned_tracks.csv")

    // tracks in playlist
    def parseTrackInPlaylist(row: String): (String, String, String) = {
      val fields = row.split(",")
      val pid = fields(0)
      val trackUri = fields(1)
      val pos = fields(2)
      (pid, trackUri, pos)
    }
    val rddTrackInPlaylist = sc.textFile(trackInPlaylistCsv).map(x => parseTrackInPlaylist(x)).distinct()
    rddTrackInPlaylist.toDF("pid", "trackUri", "pos").coalesce(1).write.option("header", "true").csv(path_to_output + "temp_cleaned_track_in_playlist.csv")

  }

}
