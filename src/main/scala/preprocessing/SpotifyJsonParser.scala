package preprocessing

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

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


  def parseLineJackson(jsonString: String): Unit = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val jsonData = mapper.readValue(jsonString, classOf[Map[String, Any]])
    val playlistRaw = jsonData("playlists").asInstanceOf[List[Map[String, Any]]]

    val playlists = playlistRaw.map { playlist =>
      Playlist(
        name = playlist("name").asInstanceOf[String],
        pid = playlist("pid").asInstanceOf[Int],
        numFollowers = playlist("num_followers").asInstanceOf[Int]
      )
    }

    val (tracks, artists, tracksInPlaylist) = playlistRaw.foldLeft(
      (List.empty[Track], List.empty[Artist], List.empty[TrackInPlaylist])
    ) {
      case ((accTracks, accArtists, accTracksInPlaylist), playlistJson) =>
        val pid = playlistJson("pid").asInstanceOf[Int]
        val tracksRaw = playlistJson("tracks").asInstanceOf[List[Map[String, Any]]]

        val newTracks = tracksRaw.map { trackJson =>
          Track(
            // in the name substitute the comma with a dot
            name = trackJson("track_name").asInstanceOf[String].replace(",", "."),
            uri = trackJson("track_uri").asInstanceOf[String],
            artistUri = trackJson("artist_uri").asInstanceOf[String],
            albumUri = trackJson("album_uri").asInstanceOf[String],
            duration = trackJson("duration_ms").asInstanceOf[Int],
            albumName = trackJson("album_name").asInstanceOf[String]
          )
        }

        val newArtists = tracksRaw.map { trackJson =>
          Artist(
            uri = trackJson("artist_uri").asInstanceOf[String],
            name = trackJson("artist_name").asInstanceOf[String]
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
    parsedPlaylists ++= playlists.sortBy(_.pid)
    parsedTracks ++= tracks.distinct.sortBy(_.name)
    parsedArtists ++= artists.distinct.sortBy(_.name)
    parsedTracksInPlaylist ++= tracksInPlaylist.sortBy(_.pid)
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

/*
import play.api.libs.json.{JsArray, Json}
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
*/
