package preprocessing

import org.apache.spark.sql.SparkSession
import preprocessing.SpotifyJsonParser.{Artist, Playlist, Track, TrackInPlaylist}

import java.nio.file.{Files, Paths}
import scala.io.Source
import utils._


object Preprocessing {
  val path_to_datasets = Config.projectDir + "/datasets/"
  val path_to_json = path_to_datasets + "spotify/data/"
  val pathToProcessed = path_to_datasets + "processed/"
  private val directoryNames = List("tracks", "playlists", "tracks_in_playlist", "artists")
  private val spark = SparkSession.builder.appName("Preprocessing")
    .getOrCreate()

  // Function that takes a list of objects and writes them on a csv file
  private def writeCsv[T](filename: String, objects: List[T]): Unit = {
    val directoryPath = Paths.get(pathToProcessed)
    if (!Files.exists(directoryPath)) {
      Files.createDirectory(directoryPath)
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
    bw.write(body)
    bw.close()
  }


  private def writeData(parsedTracks: List[Track], parsedPlaylists: List[Playlist], parsedTracksInPlaylist: List[TrackInPlaylist], parsedArtists: List[Artist]): Unit = {
    writeCsv(pathToProcessed + "/tmp_tracks.csv", parsedTracks)
    writeCsv(pathToProcessed + "/tmp_playlists.csv", parsedPlaylists)
    writeCsv(pathToProcessed + "/tmp_tracks_in_playlist.csv", parsedTracksInPlaylist)
    writeCsv(pathToProcessed + "/tmp_artists.csv", parsedArtists)
  }

  private def removeCrcAndSuccessFiles(directoryName: String): Unit = {
    Files.list(Paths.get(pathToProcessed + directoryName)).toArray.map(_.toString)
      .foreach(
        file => {
          if (file.contains(".crc") || file.contains("SUCCESS")) {
            Files.deleteIfExists(Paths.get(file))
          }
        }
      )
  }

  private def renameAndMoveCsvFile(directoryName: String): Unit = {
    Files.list(Paths.get(pathToProcessed + directoryName)).toArray.map(_.toString)
      .foreach(
        file => {
          if (file.contains("part-00000-")) {
            val newFileName = file.replaceAll("part-00000-.*", directoryName + ".csv")
            val newFilePath = newFileName.replace(s"${java.io.File.separator}$directoryName${java.io.File.separator}", s"${java.io.File.separator}")
            Files.move(Paths.get(file), Paths.get(newFilePath), java.nio.file.StandardCopyOption.REPLACE_EXISTING)
          }
        }
      )
  }

  private def removeTmpCsvFiles(): Unit = {
    Files.list(Paths.get(pathToProcessed)).toArray.map(_.toString)
      .foreach(
        file => {
          if (file.contains("tmp_")) {
            Files.deleteIfExists(Paths.get(file))
          }
        }
      )
  }

  private def deleteDirectory(directoryPath: String): Unit = {
    val dir = Paths.get(directoryPath)
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

  // main
  def main(args: Array[String]): Unit = {
    val files = Files.list(Paths.get(path_to_json)).toArray.map(_.toString)
      .take(3)
      .filterNot(_.contains(".DS_Store"))
    var i = 1
    for (file <- files) {
      print("File number: " + i + " ")
      println("Processing file: " + file)
      val source = Source.fromFile(file)
      val jsonString = try source.mkString finally source.close()
      SpotifyJsonParser.parseLineJackson(jsonString)
      println("Done processing file: " + file)
      i += 1
      val parsedData = SpotifyJsonParser.getParsedData
      writeData(parsedData._1, parsedData._2, parsedData._3, parsedData._4)
    }
    removeDuplicates()
    clearTempDirectory()
  }
}
