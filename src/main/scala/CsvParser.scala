import java.util.Calendar

object CsvParser {

  val noGenresListed = "(no genres listed)"
  val commaRegex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
  val pipeRegex = "\\|(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
  val quotes = "\""

  /** Convert from timestamp (String) to year (Int) */
  def yearFromTimestamp(timestamp: String): Int = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(timestamp.trim.toLong * 1000L)
    cal.get(Calendar.YEAR)
  }

  /** Function to parse movie records
   *
   * @param line line that has to be parsed
   * @return tuple containing movieId, title and genres, none in case of input errors
   */
  def parseMovieLine(line: String): Option[(Long, String, String)] = {
    try {
      val input = line.split(commaRegex)
      var title = input(1).trim
      title = if (title.startsWith(quotes)) title.substring(1) else title
      title = if (title.endsWith(quotes)) title.substring(0, title.length - 1) else title
      Some(input(0).trim.toLong, title, input(2).trim)
    } catch {
      case _: Exception => None
    }
  }

  // (PID, playlist_name, num_followers)
  def parsePlayListLine(line: String): Option[(String, String, Int)] = {
    try {
      val input = line.split(commaRegex)
      Some(input(0).trim, input(1).trim, input(2).trim.toInt)
    } catch {
      case _: Exception => None
    }
  }

  // (track_uri, track_name, duration_ms, artist_uri, album_uri, album_name)
  def parseTrackLine(line: String): Option[(String, String, Int, String, String, String)] = {
    try {
      val input = line.split(commaRegex)
      Some(input(0).trim, input(1).trim, input(2).trim.toInt, input(3).trim, input(4).trim, input(5).trim)
    } catch {
      case _: Exception => None
    }
  }

  // (artist_uri, artist_name)
  def parseArtistLine(line: String): Option[(String, String)] = {
    try {
      val input = line.split(commaRegex)
      Some(input(0).trim, input(1).trim)
    } catch {
      case _: Exception => None
    }
  }

  // (PID, track_uri, pos)
  def parseTrackInPlaylistLine(line: String): Option[(String, String, Int)] = {
    try {
      val input = line.split(commaRegex)
      Some(input(0).trim, input(1).trim, input(2).trim.toInt)
    } catch {
      case _: Exception => None
    }
  }
}


