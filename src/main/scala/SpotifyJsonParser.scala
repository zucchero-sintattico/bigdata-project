import play.api.libs.json.{JsArray, Json}

object SpotifyJsonParser {
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

  private var parsedTracks = List.empty[Track]
  private var parsedPlaylists = List.empty[Playlist]
  private var parsedTracksInPlaylist = List.empty[TrackInPlaylist]
  private var parsedArtists = List.empty[Artist]

  def parseLine(jsonString: String
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

  def getParsedData: (List[Track], List[Playlist], List[TrackInPlaylist], List[Artist]) = {
    // Return the parsed data copied and clear the parsed lists
    val data = (List(parsedTracks: _*), List(parsedPlaylists: _*), List(parsedTracksInPlaylist: _*), List(parsedArtists: _*))
    parsedTracks = List.empty[Track]
    parsedPlaylists = List.empty[Playlist]
    parsedTracksInPlaylist = List.empty[TrackInPlaylist]
    parsedArtists = List.empty[Artist]
    data
  }
}
