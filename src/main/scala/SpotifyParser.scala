import java.nio.file.{FileSystems, Files}
import scala.io.{BufferedSource, Source}
import play.api.libs.json._


object SpotifyParser {
  case class Track(
                    uri: String,
                    name: String,
                    duration: Int,
                    artistUri: String,
                    albumUri: String,
                    albumName: String
                  )

  case class Playlist(
                       pid: Int,
                       name: String,
                       numFollowers: Int
                     )

  case class TrackInPlaylist(
                              pid: Int,
                              trackUri: String,
                              pos: Int
                            )

  case class Artist(
                     uri: String,
                     name: String
                   )

  val path_to_datasets = "dataset/spotify/data/"

  // create a filenames variable to store all the filenames in the directory
  //  val fileNames = Files.list(FileSystems.getDefault.getPath(path_to_datasets)).toArray.map(_.toString)

  //  val file = path_to_datasets + "test.json"
  val file = path_to_datasets + "mpd.slice.0-999.json"

  def parseLine(jsonString: String): (List[Track], List[Artist], List[Playlist], List[TrackInPlaylist]) = {
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

        val newArtists = newTracks.map { track =>
          Artist(
            uri = track.artistUri,
            name = (tracksJson.value.head \ "artist_name").as[String]
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

    // Remove duplicates and return
    (tracks.distinct, artists.distinct, playlists, tracksInPlaylist.distinct)
  }

  // Function that takes a list of objects and writes them on a csv file
  def writeCsv[T](filename: String, objects: List[T]): Unit = {
    val header = objects.head.getClass.getDeclaredFields.map(_.getName).mkString(",") + "\n"
    val body = objects.map { obj =>
      obj.getClass.getDeclaredFields.map { field =>
        field.setAccessible(true)
        field.get(obj).toString
      }.mkString(",")
    }.mkString("\n")

    val file = new java.io.File(filename)
    val bw = new java.io.BufferedWriter(new java.io.FileWriter(file))
    bw.write(header + body)
    bw.close()
  }

  def main(args: Array[String]): Unit = {
    //    import java.io.File
    //
    //    val currentDir = new File(".")
    //    println("Files in current directory: " + currentDir.listFiles().map(_.getName).mkString(", "))

    val jsonString = Source.fromFile(file).getLines().mkString

    val (tracks, artists, playlist, tracksInPlaylist) = parseLine(jsonString)

    writeCsv("tracks.csv", tracks)
    writeCsv("artists.csv", artists)
    writeCsv("playlist.csv", playlist)
    writeCsv("tracksInPlaylist.csv", tracksInPlaylist)

    println("Playlist: " + playlist)
    println("Tracks: " + tracks)
    println("Artists: " + artists)
    println("Tracks in Playlist: " + tracksInPlaylist)
  }


}
