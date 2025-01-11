import SpotifyParser.path_to_datasets

import java.nio.file.{FileSystems, Files}
import scala.io.Source

object Preprocessing {

  val path_to_datasets: String = SpotifyParser.path_to_datasets
  private val files = Files.list(FileSystems.getDefault.getPath(path_to_datasets + "spotify/data/")).toArray.map(_.toString)
    .take(2)
    .filterNot(_.contains(".DS_Store"))

  // main
  def main(args: Array[String]): Unit = {
    var i = 1
    for (file <- files) {
      print("File number: " + i + " ")
      println("Processing file: " + file)
      val source = Source.fromFile(file)
      val jsonString = try source.mkString finally source.close()
      SpotifyParser.parseLine(jsonString)
      println("Done processing file: " + file)
      i += 1
      SpotifyParser.writeParsedData()
    }
    SpotifyParser.removeDuplicates()
    SpotifyParser.clearTempDirectory()
  }
}
