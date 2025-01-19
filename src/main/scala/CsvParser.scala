import java.util.Calendar

object CsvParser {

  val noGenresListed = "(no genres listed)"
  val commaRegex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
  val pipeRegex = "\\|(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
  val quotes = "\""

  case class PlayList(PID: String, playlist_name: String, num_followers: Int)

  case class Track(track_uri: String, track_name: String, duration_ms: Int, artist_uri: String, album_uri: String, album_name: String)

  case class Artist(artist_uri: String, artist_name: String)

  case class TrackInPlaylist(PID: String, track_uri: String, pos: Int)

  // (PID, playlist_name, num_followers)
  def parsePlayListLine(line: String): Option[PlayList] = {
    try {
      val input = line.split(commaRegex)
      Some(PlayList(input(0).trim, input(1).trim, input(2).trim.toInt))
    } catch {
      case _: Exception => None
    }
  }

  // (track_uri, track_name, duration_ms, artist_uri, album_uri, album_name)
  def parseTrackLine(line: String): Option[Track] = {
    try {
      val input = line.split(commaRegex)
      Some(Track(input(0).trim, input(1).trim, input(2).trim.toInt, input(3).trim, input(4).trim, input(5).trim))
    } catch {
      case _: Exception => None
    }
  }

  // (artist_uri, artist_name)
  def parseArtistLine(line: String): Option[Artist] = {
    try {
      val input = line.split(commaRegex)
      Some(Artist(input(0).trim, input(1).trim))
    } catch {
      case _: Exception => None
    }
  }

  // (PID, track_uri, pos)
  def parseTrackInPlaylistLine(line: String): Option[TrackInPlaylist] = {
    try {
      val input = line.split(commaRegex)
      Some(TrackInPlaylist(input(0).trim, input(1).trim, input(2).trim.toInt))
    } catch {
      case _: Exception => None
    }
  }
}


