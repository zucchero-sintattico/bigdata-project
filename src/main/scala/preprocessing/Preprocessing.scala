package preprocessing

import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.URI
import java.nio.file.{Files, Paths}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import preprocessing.SpotifyJsonParser.{Artist, Playlist, Track, TrackInPlaylist}
import utils.Commons.DeploymentMode.DeploymentMode
import utils.Commons

import scala.io.Source

object Preprocessing {

  private val datasetsPath: String = "/datasets/"
  private val jsonPath: String = s"${datasetsPath}spotify/data/"
  private val processedPath: String = s"${datasetsPath}processed/"
  private val csvNames: List[String] = List("tracks", "playlists", "tracks_in_playlist", "artists")
  private val spark: SparkSession = SparkSession.builder.appName("Preprocessing").getOrCreate()
  private var fs: FileSystem = _
  private var deploymentMode: String = _

  private def getDatasetPath(relativePath: String): String =
    Commons.getDatasetPath(deploymentMode, relativePath)

  private def writeCsv[T](basePath: String, objects: List[T]): Unit = {
    val outputDir = new Path(getDatasetPath(processedPath))
    if (!fs.exists(outputDir)) fs.mkdirs(outputDir)

    val timestamp = System.currentTimeMillis()
    val fileName = s"$basePath/part-$timestamp.csv"
    val filePath = new Path(fileName)

    val csvContent = objects.map { obj =>
      obj.getClass.getDeclaredFields.map { field =>
        field.setAccessible(true)
        Option(field.get(obj)).map(_.toString).getOrElse("")
      }.mkString(",")
    }.mkString("\n")

    val outputStream = fs.create(filePath, true)
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"))

    try {
      writer.write(csvContent)
    } finally {
      writer.close()
    }
  }

  /**
   * Merge all the part files from tmp directory into a single file in the merged/ directory
   *
   * @param name , the name of the directory to merge
   */
  private def mergeFiles(name: String): Unit = {
    val inputPath = s"${getDatasetPath(processedPath)}tmp_$name/part-*"
    val mergedDF = spark.read
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(inputPath)

    mergedDF
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "false")
      .save(s"${getDatasetPath(processedPath)}merged_$name")
  }

  private def writeData(tracks: List[Track],
                        playlists: List[Playlist],
                        tracksInPlaylist: List[TrackInPlaylist],
                        artists: List[Artist]): Unit = {
    writeCsv(getDatasetPath(s"$processedPath/tmp_tracks"), tracks)
    writeCsv(getDatasetPath(s"$processedPath/tmp_playlists"), playlists)
    writeCsv(getDatasetPath(s"$processedPath/tmp_tracks_in_playlist"), tracksInPlaylist)
    writeCsv(getDatasetPath(s"$processedPath/tmp_artists"), artists)
  }

  /**
   * Remove the crc and success files from the directory
   *
   * @param directoryName , the name of the directory to clean
   */
  private def removeCrcAndSuccessFiles(directoryName: String): Unit = {
    val dirPath = new Path(getDatasetPath(s"$processedPath$directoryName"))
    val files = fs.listStatus(dirPath)
    files.foreach { fileStatus =>
      val fileName = fileStatus.getPath.getName
      if (fileName.contains(".crc") || fileName.contains("SUCCESS") || fileName.startsWith(".")) {
        if (fs.exists(fileStatus.getPath)) {
          fs.delete(fileStatus.getPath, false)
          println(s"File ${fileStatus.getPath} eliminato")
        }
      }
    }
  }

  /**
   * Rename the "part-..." file and move it to the root of the directory
   *
   * @param directoryName , the name of the directory to clean
   */
  private def renameAndMoveCsvFile(directoryName: String): Unit = {
    val dirPath = new Path(getDatasetPath(s"$processedPath$directoryName"))
    fs.listStatus(dirPath).foreach { fileStatus =>
      val filePathStr = fileStatus.getPath.toString
      if (filePathStr.contains("part-00000-")) {
        val newFileName = filePathStr.replaceAll("part-00000-.*", s"$directoryName.csv")
        val newFilePath = new Path(newFileName.replace(s"${java.io.File.separator}$directoryName${java.io.File.separator}", s"${java.io.File.separator}"))
        if (fs.exists(newFilePath)) fs.delete(newFilePath, false)
        if (fs.exists(fileStatus.getPath)) {
          fs.rename(fileStatus.getPath, newFilePath)
          println(s"File $filePathStr rinominato e spostato in $newFilePath")
        }
      }
    }
  }


  private def removeTemporaryFiles(): Unit = {
    val baseDir = new Path(getDatasetPath(processedPath))
    fs.listStatus(baseDir).foreach { fileStatus =>
      val filePathStr = fileStatus.getPath.toString
      if (filePathStr.contains("tmp_") || filePathStr.contains("merged_")) {
        if (fs.exists(fileStatus.getPath)) {
          fs.delete(fileStatus.getPath, true)
          println(s"File temporaneo $filePathStr rimosso.")
        }
      }
    }
  }

  private def deleteDirectory(relativeDir: String): Unit = {
    val dirPath = new Path(getDatasetPath(relativeDir))
    if (fs.exists(dirPath)) {
      fs.delete(dirPath, true)
      println(s"Directory $relativeDir eliminata.")
    }
  }

  private def clearTempDirectory(): Unit = {
    csvNames.foreach { name =>
      removeCrcAndSuccessFiles(s"/merged_$name")
      renameAndMoveCsvFile(name)
      deleteDirectory(s"$processedPath$name")
    }
    removeTemporaryFiles()
  }

  private def removeDuplicates(): Unit = {
    csvNames.foreach { name =>
      val df = spark.read.option("header", "false").csv(s"${getDatasetPath(processedPath)}merged_$name")
      val sortedDF = name match {
        case "tracks_in_playlist" | "playlists" =>
          df.withColumn("_c0", col("_c0").cast("int")).orderBy("_c0")
        case _ =>
          df.distinct().orderBy("_c1")
      }
      sortedDF.coalesce(1).write.mode("overwrite").csv(s"${getDatasetPath(processedPath)}$name")
    }
  }

  private def initializeFs(deploymentMode: String): Unit = {
    fs = if (deploymentMode == "remote") {
      FileSystem.get(new URI(getDatasetPath(jsonPath)), spark.sparkContext.hadoopConfiguration)
    } else {
      FileSystem.get(spark.sparkContext.hadoopConfiguration)
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Inserire il deployment mode (local o remote)")
      sys.exit(1)
    }

    deploymentMode = args(0)
    Commons.initializeSparkContext(deploymentMode, spark)
    initializeFs(deploymentMode)

    val jsonFiles: List[String] = {
      val basePath = getDatasetPath(jsonPath)
      val path = new Path(basePath)
      if (deploymentMode == "local") {
        Files.list(Paths.get(basePath))
          .toArray
          .map(_.toString)
          .filter(_.endsWith(".json"))
          .take(10)
          .toList
      } else {
        fs.listStatus(path).map(_.getPath.toString).toList
      }
    }

    println(s"Files to process: ${jsonFiles.size}")
    var counter = 1

    jsonFiles.foreach { file =>
      println(s"File number: $counter. Processing file: $file")
      val jsonString = Source.fromInputStream(fs.open(new Path(file))).mkString
      SpotifyJsonParser.parseLineJackson(jsonString)
      println(s"Done processing file: $file")
      counter += 1

      val (parsedTracks, parsedPlaylists, parsedTracksInPlaylist, parsedArtists) = SpotifyJsonParser.getParsedData
      writeData(parsedTracks, parsedPlaylists, parsedTracksInPlaylist, parsedArtists)
    }

    csvNames.foreach(mergeFiles)
    removeDuplicates()
    clearTempDirectory()
  }
}
