package preprocessing

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import preprocessing.SpotifyJsonParser.{Artist, Playlist, Track, TrackInPlaylist}
import utils.Commons.DeploymentMode.DeploymentMode

import java.nio.file.{Files, Paths}
import scala.io.Source
import utils._

import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.URI

object Preprocessing {
  val path_to_datasets = "/datasets/"
  val path_to_json = path_to_datasets + "spotify/data/"
  val pathToProcessed = path_to_datasets + "processed/"
  private val directoryNames = List("tracks", "playlists", "tracks_in_playlist", "artists")
  private val spark = SparkSession.builder.appName("Preprocessing").getOrCreate()
  var fs: FileSystem = _


  private var deploymentMode: String = _

  private def getDatasetPath(path: String): String = {
    Commons.getDatasetPath(deploymentMode, path)
  }

  private def writeCsv[T](filename: String, objects: List[T]): Unit = {
    val directoryPath = Paths.get(getDatasetPath(pathToProcessed))
    if (!Files.exists(directoryPath)) {
      //      Files.createDirectory(directoryPath)
      fs.mkdirs(new Path(getDatasetPath(pathToProcessed)))
    }
    val body = objects.map { obj =>
      obj.getClass.getDeclaredFields.map { field =>
        field.setAccessible(true)
        field.get(obj).toString
      }.mkString(",")
    }.mkString("\n")
    val outputStream = fs.create(new Path(filename), true) // true per sovrascrivere se esiste
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"))

    try {
      writer.write(body + "\n")
    } finally {
      writer.close()
    }
  }

  private def writeData(parsedTracks: List[Track], parsedPlaylists: List[Playlist], parsedTracksInPlaylist: List[TrackInPlaylist], parsedArtists: List[Artist]): Unit = {
    writeCsv(getDatasetPath(pathToProcessed + "/tmp_tracks.csv"), parsedTracks)
    writeCsv(getDatasetPath(pathToProcessed + "/tmp_playlists.csv"), parsedPlaylists)
    writeCsv(getDatasetPath(pathToProcessed + "/tmp_tracks_in_playlist.csv"), parsedTracksInPlaylist)
    writeCsv(getDatasetPath(pathToProcessed + "/tmp_artists.csv"), parsedArtists)
  }

  private def removeCrcAndSuccessFiles(directoryName: String): Unit = {
    val directoryPath = new Path(getDatasetPath(pathToProcessed + directoryName))

    // Elenco dei file nel bucket S3
    val files = fs.listStatus(directoryPath)

    // Itera sui file
    files.foreach { file =>
      val filePath = file.getPath.toString

      // Se il file contiene .crc o SUCCESS, lo rimuovi
      if (filePath.contains(".crc") || filePath.contains("SUCCESS")) {
        if (fs.exists(file.getPath)) {
          fs.delete(file.getPath, false) // false indica che non è una cartella
          println(s"File $filePath eliminato")
        }
      }
    }
  }

  private def renameAndMoveCsvFile(directoryName: String): Unit = {
    val directoryPath = new Path(getDatasetPath(pathToProcessed) + directoryName)

    // Elenco dei file nel bucket S3
    val files = fs.listStatus(directoryPath)

    // Itera sui file
    files.foreach { file =>
      val filePath = file.getPath.toString

      // Se il file contiene "part-00000-", lo rinomini
      if (filePath.contains("part-00000-")) {
        val newFileName = filePath.replaceAll("part-00000-.*", directoryName + ".csv")
        val newFilePath = newFileName.replace(s"${java.io.File.separator}$directoryName${java.io.File.separator}", s"${java.io.File.separator}")

        // Rinominare e spostare il file
        val oldPath = new Path(filePath)
        val newPath = new Path(newFilePath)
        if (fs.exists(oldPath)) {
          fs.rename(oldPath, newPath)
          println(s"File $filePath rinominato e spostato in $newFilePath")
        }
      }
    }
  }

  private def removeTmpCsvFiles(): Unit = {
    val directoryPath = new Path(getDatasetPath(pathToProcessed))

    // Elenco dei file nella directory S3
    val files = fs.listStatus(directoryPath)

    // Itera sui file e rimuovi quelli temporanei
    files.foreach { file =>
      val filePath = file.getPath.toString
      if (filePath.contains("tmp_")) {
        val pathToDelete = new Path(filePath)
        if (fs.exists(pathToDelete)) {
          fs.delete(pathToDelete, false) // 'false' per non rimuovere ricorsivamente
          println(s"File temporaneo $filePath rimosso.")
        }
      }
    }
  }

  private def deleteDirectory(directoryPath: String): Unit = {
    val dirPath = new Path(getDatasetPath(directoryPath))

    if (fs.exists(dirPath)) {
      fs.delete(dirPath, true) // 'true' per eliminare la directory e tutto il suo contenuto
      println(s"Directory $directoryPath eliminata.")
    }
  }

  private def clearTempDirectory(): Unit = {
    directoryNames.foreach { directory =>
      removeCrcAndSuccessFiles(directory)
      renameAndMoveCsvFile(directory)
      deleteDirectory(pathToProcessed + directory)
    }
    removeTmpCsvFiles()
  }

  private def removeDuplicates(): Unit = {
    for (directory <- directoryNames) {
      val df = spark.read.option("header", "false").csv(getDatasetPath(pathToProcessed) + "tmp_" + directory + ".csv")

      val sortedDF = if (directory == "tracks_in_playlist" || directory == "playlists") {
        df.withColumn("_c0", col("_c0").cast("int")).orderBy("_c0")
      } else {
        df.distinct().orderBy("_c1")
      }

      sortedDF.coalesce(1).write.mode("overwrite").csv(getDatasetPath(pathToProcessed) + directory)
    }
  }

  def main(args: Array[String]): Unit = {
    deploymentMode = args(0)

    Commons.initializeSparkContext(deploymentMode, spark)
    initializeFs(deploymentMode)
    var files = List.empty[String]
    val path = new Path(getDatasetPath(path_to_json))
    if (deploymentMode == "local") {
      files = Files.list(Paths.get(getDatasetPath(path_to_json))).toArray.map(_.toString).filter(_.endsWith(".json")).toList
    }
    else {
      files = fs.listStatus(path).map(_.getPath.toString).toList
    }
    var i = 1
    for (file <- files) {
      print("File number: " + i + " ")
      println("Processing file: " + file)
      //      val source = Source.fromFile(file)
      val jsonString = Source.fromInputStream(fs.open(new Path(file))).mkString
      //      val jsonString = try source.mkString finally source.close()
      SpotifyJsonParser.parseLineJackson(jsonString)
      println("Done processing file: " + file)
      i += 1
      val parsedData = SpotifyJsonParser.getParsedData
      writeData(parsedData._1, parsedData._2, parsedData._3, parsedData._4)
    }
    removeDuplicates()
    clearTempDirectory()
  }

  private def initializeFs(deploymentMode: String): Unit = {
    if (deploymentMode == "remote") {
      fs = FileSystem.get(new URI(getDatasetPath(path_to_json)), spark.sparkContext.hadoopConfiguration)
    }
    else {
      fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    }

  }
}