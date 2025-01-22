import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.{Commons, Config}

import scala.collection.mutable

object Job {


  val path_to_datasets = "/datasets/processed/"

  private val path_tracks = path_to_datasets + "tracks.csv"
  private val path_playlists = path_to_datasets + "playlists.csv"
  private val path_tracks_in_playlist = path_to_datasets + "tracks_in_playlist.csv"
  private val path_artists = path_to_datasets + "artists.csv"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spotify job").getOrCreate()
    val sqlContext = spark.sqlContext // needed to save as CSV


    //    if (args.length < 2) {
    //      println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
    //      println("The second parameter should indicate the job (1 for join-and-agg, 2 for agg-and-join, 3 for agg-and-bjoin)")
    //      return
    //    }

    val deploymentMode = args(0)
    var writeMode = deploymentMode
    if (deploymentMode == "sharedRemote") {
      writeMode = "remote"
    }
    //    val job = args(1)
    val job = "4"
    val rddTracks = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_tracks)).
      flatMap(CsvParser.parseTrackLine)

    val rddPlaylists = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_playlists)).
      flatMap(CsvParser.parsePlayListLine)

    val rddTracksInPlaylist = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_tracks_in_playlist)).
      flatMap(CsvParser.parseTrackInPlaylistLine)

    val rddArtists = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_artists)).
      flatMap(CsvParser.parseArtistLine)

    if (job == "1") {
      // For each artist, calculate the average number of songs in a playlist
      //      val rddTracksKV = rddTracks.map(x => (x.track_uri, x.artist_uri))
      //      val rddPlaylistsKV = rddPlaylists.map(x => (x.PID, 1))
      //      val rddArtistKV = rddArtists.map(x => (x.artist_uri, 1))
      //      val rddTracksInPlaylistKV = rddTracksInPlaylist.map(x => (x.track_uri, x.PID))
      val rddTracksKV = rddTracks.map(x => (x._1, x._4))
      val rddPlaylistsKV = rddPlaylists.map(x => (x._1, 1))
      val rddArtistKV = rddArtists.map(x => (x._1, 1))
      val rddTracksInPlaylistKV = rddTracksInPlaylist.map(x => (x._2, x._1))

      import spark.implicits._
      val rddTrackPlaylist = rddTracksInPlaylistKV.join(
        rddTracksKV
      )
      val rddTrackPlaylistArtist = rddTrackPlaylist.join(rddArtistKV)
      val artistSongCount =
        rddTrackPlaylistArtist
          .map(x => (x._2._2, x._2._1))
          .groupByKey()
          .mapValues(_.size)
          .collect()
      // save output as CSV
      val artistSongCountDF = artistSongCount.toSeq.toDF("artist_uri", "num_songs").write.format("csv").mode(SaveMode.Overwrite).save(Commons.getDatasetPath(writeMode, "/output/artist_song_count"))
      //      val avgSongOfEachArtistInAPlaylist = rddTracksInPlaylist.
      //        //        map(x => (x.track_uri, x.PID)).
      //        //        join(rddTracksKV).
      //        //        map(x => (x._2._2, x._2._1)).
      //        //        join(rddPlaylistsKV).
      //        //        map(x => (x._2._1, x._2._2)).
      //        //        reduceByKey(_ + _).
      //        //        join(rddArtistKV).
      //        //        map(x => (x._1, x._2._1.toDouble / x._2._2)).
      //        collect()

      // save output as CSV
    }
    else if (job == "2") {
      // Job Tommi

      // RDD of (pid, trackUri)
      val playlistTracks = rddTracksInPlaylist.map(x => (x._1, x._2))

      // auto-join on track_in_playlist to get all song pairs in the same playlist
      val rddTrackPairs = playlistTracks.join(playlistTracks)
        .filter { case (_, (track1, track2)) => track1 != track2 }
        .map { case (_, (track1, track2)) => ((track1, track2), 1)}

      // => rdd di forma ((track1,track2), 1) use to count the occurrences

      val occurrencesCount = rddTrackPairs.reduceByKey(_ + _)

      // take the pair with the highest value for each track
      val mostOccurrencesPair = occurrencesCount
        .map { case ((track1, track2), count) => (track1, (track2, count))}
        .reduceByKey { case ((track2, count1), (track3, count2)) => if (count1 > count2) (track2, count1) else (track3, count2)}

      // join on rddTracks for track names
      val trackDetails = rddTracks.map(x => (x._1, x._2))
      val firstResult = mostOccurrencesPair.join(trackDetails)
        .map { case (track1, ((track2, count), track1Name)) => (track1, track1Name, track2, count)}

      val result = firstResult
        .map { case (track1, name, track2, count) => (track2, (track1, name, count)) }
        .join(trackDetails)
        .map { case (track2, ((track1, name, count), track2Name)) => (track1, name, track2, track2Name, count) }

      result.saveAsTextFile(Config.projectDir + "output/result")

    }
    else if (job == "3") {
      // Job Gigi Optimized
    }
    else if (job == "4") {
      // Job Tommi optimized

      val playlistToTracks = rddTracksInPlaylist
        .map(x => (x._1, x._2))

      val coTracksByPlaylist = playlistToTracks
        .groupByKey() // group the track for same playlist
        .flatMap { case (_, tracks) =>
          val trackList = tracks.toList
          for {
            track <- trackList
            coTrack <- trackList if track != coTrack
          } yield (track, coTrack) // generates pair (track, coTrack)
        }


      val occurrenceCount = coTracksByPlaylist
        .aggregateByKey(mutable.Map[String, Int]())(    // ogni chiave (traccia) viene inizializzata con una mappa che terrà il count delle sue co-tracce
          (acc, coTrack) => {
            acc(coTrack) = acc.getOrElse(coTrack, 0) + 1
            acc
          },   // per ogni chiave vengono iterati i valori associati (coTrack) e incrementato il count
          (map1, map2) => {
            map2.foreach { case (coTrack, count) =>
              map1(coTrack) = map1.getOrElse(coTrack, 0) + count
            }
            map1
          }
        )
        .mapValues(_.toMap)

      /*
      val occurrenceCount = coTracksByPlaylist
        .map { case (track, coTrack) => (track, Map(coTrack -> 1)) } // Mappa tracce a un conteggio iniziale
        .aggregateByKey(Map[String, Int]())(
          (acc, value) => { // Combinatore locale
            value.foldLeft(acc) { case (map, (coTrack, count)) =>
              map + (coTrack -> (map.getOrElse(coTrack, 0) + count))
            }
          },
          (map1, map2) => { // Combinatore globale
            map2.foldLeft(map1) { case (map, (coTrack, count)) =>
              map + (coTrack -> (map.getOrElse(coTrack, 0) + count))
            }
          }
        )

       */
      val mostCooccurringTrackPerTrack = occurrenceCount
        .mapValues { occurrences =>
          occurrences.maxBy(_._2) // Trova la traccia con il conteggio massimo
        }

      // Mappa i dettagli delle tracce
      val trackDetail = rddTracks.map(line => (line._1, line._2))

      // Aggiungi i dettagli
      val enrichedResults = mostCooccurringTrackPerTrack
        .join(trackDetail) // Aggiungi il nome della traccia principale
        .map { case (trackUri, ((coTrackUri, count), trackName)) =>
          (coTrackUri, (trackUri, trackName, count))
        }
        .join(trackDetail) // Aggiungi il nome della traccia co-occurrente
        .map { case (coTrackUri, ((trackUri, trackName, count), coTrackName)) =>
          (trackUri, trackName, coTrackUri, coTrackName, count)
        }

      enrichedResults.saveAsTextFile(Config.projectDir + "output/result")

    }
  }

}
