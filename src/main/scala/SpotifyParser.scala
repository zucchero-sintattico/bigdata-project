import org.apache.spark.sql.SparkSession

import java.nio.file.{FileSystems, Files}
import scala.io.Source
import play.api.libs.json._


object SpotifyParser {


  private case class Track(
                            uri: String,
                            name: String,
                            duration: Int,
                            artistUri: String,
                            albumUri: String,
                            albumName: String
                          )

  private case class Playlist(
                               pid: Int,
                               name: String,
                               numFollowers: Int
                             )

  private case class TrackInPlaylist(
                                      pid: Int,
                                      trackUri: String,
                                      pos: Int
                                    )

  private case class Artist(
                             uri: String,
                             name: String
                           )

  // tommi
  //  private val path_to_datasets = "C:/Users/tbrin/Desktop/bigdata-project/datasets/"

  // giggi
  val path_to_datasets = "/Users/giggino/Desktop/bigdata-project/datasets/"
  val pathToProcessed = path_to_datasets + "processed/"
  // create a filenames variable to store all the filenames in the directory
  //  val fileNames = Files.list(FileSystems.getDefault.getPath(path_to_datasets)).toArray.map(_.toString)

  //  val file = path_to_datasets + "test.json"

  private val directoryNames = List("tracks", "playlists", "tracks_in_playlist", "artists")
  private var parsedTracks = List.empty[Track]
  private var parsedPlaylists = List.empty[Playlist]
  private var parsedTracksInPlaylist = List.empty[TrackInPlaylist]
  private var parsedArtists = List.empty[Artist]
  private val spark = SparkSession.builder.appName("SpotifyParser")
    .config("spark.hadoop.fs.s3a.createCrc", "false") // Disabilita i .crc per S3
    .config("spark.hadoop.fs.local.createCrc", "false") // Disabilita i .crc per il file system locale.getOrCreate()
    .getOrCreate()

  private def parseLine(jsonString: String
                       ): Unit = {
    val json = Json.parse(jsonString)

    // Extract all playlists
    val playlistsJson = (json \ "playlists").as[JsArray]

    val playlists = playlistsJson.value.map { playlistJson =>
      Playlist(
        name = (playlistJson \ "name").as[String],
        pid = (playlistJson \ "pid").as[Int],
        numFollowers = (playlistJson \ "num_followers").as[Int]
      )
    }.toList

    // Extract Tracks, Artists, and TracksInPlaylist for all playlists
    val (tracks, artists, tracksInPlaylist) = playlistsJson.value.foldLeft(
      (List.empty[Track], List.empty[Artist], List.empty[TrackInPlaylist])
    ) {
      case ((accTracks, accArtists, accTracksInPlaylist), playlistJson) =>
        val pid = (playlistJson \ "pid").as[Int]
        val tracksJson = (playlistJson \ "tracks").as[JsArray]

        val newTracks = tracksJson.value.map { trackJson =>
          Track(
            name = (trackJson \ "track_name").as[String],
            uri = (trackJson \ "track_uri").as[String],
            artistUri = (trackJson \ "artist_uri").as[String],
            albumUri = (trackJson \ "album_uri").as[String],
            duration = (trackJson \ "duration_ms").as[Int],
            albumName = (trackJson \ "album_name").as[String]
          )
        }.toList

        val newArtists = tracksJson.value.map { trackJson =>
          Artist(
            uri = (trackJson \ "artist_uri").as[String],
            name = (trackJson \ "artist_name").as[String]
          )
        }

        val newTracksInPlaylist = newTracks.zipWithIndex.map { case (track, index) =>
          TrackInPlaylist(
            pid = pid,
            trackUri = track.uri,
            pos = index
          )
        }

        (
          accTracks ++ newTracks,
          accArtists ++ newArtists,
          accTracksInPlaylist ++ newTracksInPlaylist
        )
    }

    // Remove duplicates and add to the parsed lists
    parsedPlaylists ++= playlists
    parsedTracks ++= tracks.distinct
    parsedArtists ++= artists.distinct
    parsedTracksInPlaylist ++= tracksInPlaylist
  }

  // Function that takes a list of objects and writes them on a csv file
  private def writeCsv[T](filename: String, objects: List[T]): Unit = {
    val header = objects.headOption match {
      case Some(obj) => obj.getClass.getDeclaredFields.map(_.getName).mkString(",") + "\n"
      case None => ""
    }

    val body = objects.map { obj =>
      obj.getClass.getDeclaredFields.map { field =>
        field.setAccessible(true)
        field.get(obj).toString
      }.mkString(",")
    }.mkString("\n")

    // open file and overwrite if it exists
    val bw = new java.io.BufferedWriter(
      new java.io.FileWriter(filename, true)) // false to overwrite
    bw.write(header + body)
    bw.close()
  }


  private def writeParsedData(): Unit = {
    writeCsv(pathToProcessed + "/tmp_tracks.csv", parsedTracks)
    writeCsv(pathToProcessed + "/tmp_playlists.csv", parsedPlaylists)
    writeCsv(pathToProcessed + "/tmp_tracks_in_playlist.csv", parsedTracksInPlaylist)
    writeCsv(pathToProcessed + "/tmp_artists.csv", parsedArtists)
    parsedTracks = List.empty[Track]
    parsedPlaylists = List.empty[Playlist]
    parsedTracksInPlaylist = List.empty[TrackInPlaylist]
    parsedArtists = List.empty[Artist]
  }

  private def removeCrcAndSuccessFiles(directoryName: String): Unit = {
    Files.list(FileSystems.getDefault.getPath(pathToProcessed + directoryName)).toArray.map(_.toString)
      .foreach(
        file => {
          if (file.contains(".crc") || file.contains("SUCCESS")) {
            Files.deleteIfExists(FileSystems.getDefault.getPath(file))
          }
        }
      )
  }

  private def renameAndMoveCsvFile(directoryName: String): Unit = {
    Files.list(FileSystems.getDefault.getPath(pathToProcessed + directoryName)).toArray.map(_.toString)
      .foreach(
        file => {
          if (file.contains("part-00000-")) {
            val newFileName = file.replaceAll("part-00000-.*", directoryName + ".csv")
            val newFilePath = newFileName.replace(s"${java.io.File.separator}$directoryName${java.io.File.separator}", s"${java.io.File.separator}")
            Files.move(FileSystems.getDefault.getPath(file), FileSystems.getDefault.getPath(newFilePath), java.nio.file.StandardCopyOption.REPLACE_EXISTING)
          }
        }
      )
  }

  private def removeTmpCsvFiles(): Unit = {
    Files.list(FileSystems.getDefault.getPath(pathToProcessed)).toArray.map(_.toString)
      .foreach(
        file => {
          if (file.contains("tmp_")) {
            Files.deleteIfExists(FileSystems.getDefault.getPath(file))
          }
        }
      )
  }

  private def deleteDirectory(directoryPath: String): Unit = {
    val dir = FileSystems.getDefault.getPath(directoryPath)
    Files.deleteIfExists(dir)
  }

  private def clearTempDirectory(): Unit = {
    directoryNames.foreach(
      directory => {
        removeCrcAndSuccessFiles(directory)
        renameAndMoveCsvFile(directory)
        deleteDirectory(pathToProcessed + directory)
      }
    )
    removeTmpCsvFiles()
  }

  private def removeDuplicates(): Unit = {
    for (directory <- directoryNames) {
      val df = spark.read.option("header", "true").csv(pathToProcessed + "tmp_" + directory + ".csv")
      df.distinct().coalesce(1).write.mode("overwrite").csv(pathToProcessed + directory)
    }
  }


  def main(args: Array[String]): Unit = {

    //    import java.io.File
    //
    //    val currentDir = new File(".")
    //    println("Files in current directory: " + currentDir.listFiles().map(_.getName).mkString(", "))
    val files = Files.list(FileSystems.getDefault.getPath(path_to_datasets + "spotify/data/")).toArray.map(_.toString).take(2)
    // remove .DS_Store file
    val filesFiltered = files.filterNot(_.contains(".DS_Store"))
    var i = 1
    for (file <- filesFiltered) {
      print("File number: " + i + " ")
      println("Processing file: " + file)
      val source = Source.fromFile(file)
      val jsonString = try source.mkString finally source.close()
      parseLine(jsonString)
      println("Done processing file: " + file)
      i += 1
      writeParsedData()
    }
    removeDuplicates()
    clearTempDirectory()


  }


}
