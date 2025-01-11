import org.apache.spark.sql.SparkSession

import java.nio.file.{FileSystems, Files, Paths}
import scala.io.Source
import SpotifyJsonParser.{Artist, Playlist, Track, TrackInPlaylist}

import javax.sound.midi

object Preprocessing {
  // tommi
  private val path_to_datasets = "C:/Users/tbrin/Desktop/bigdata-project/datasets/"
  // giggi
  //val path_to_datasets = "/Users/giggino/Desktop/bigdata-project/datasets/"
  val pathToProcessed = path_to_datasets + "processed/"
  private val directoryNames = List("tracks", "playlists", "tracks_in_playlist", "artists")
  private val spark = SparkSession.builder.appName("Preprocessing")
    .config("spark.hadoop.fs.s3a.createCrc", "false") // Disabilita i .crc per S3
    .config("spark.hadoop.fs.local.createCrc", "false") // Disabilita i .crc per il file system locale.getOrCreate()
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

  // main
  def main(args: Array[String]): Unit = {
    val files = Files.list(FileSystems.getDefault.getPath(path_to_datasets + "spotify/data/")).toArray.map(_.toString)
      .take(5)
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
