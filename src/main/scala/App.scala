import Preprocessing.path_to_datasets

import java.nio.file.{FileSystems, Files}
import scala.io.Source

object App {

  val path_to_datasets: String = Preprocessing.path_to_datasets
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
      Preprocessing.parseLine(jsonString)
      println("Done processing file: " + file)
      i += 1
      Preprocessing.writeParsedData()
    }
    Preprocessing.removeDuplicates()
    Preprocessing.clearTempDirectory()
  }
}
