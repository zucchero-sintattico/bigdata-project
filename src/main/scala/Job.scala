import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.{Commons, Config}


object Job {


  val path_to_datasets = "/datasets/processed/"

  private val path_tracks = path_to_datasets + "tracks.csv"
  private val path_playlists = path_to_datasets + "playlists.csv"
  private val path_tracks_in_playlist = path_to_datasets + "tracks_in_playlist.csv"
  private val path_artists = path_to_datasets + "artists.csv"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spotify job").getOrCreate()
    val sqlContext = spark.sqlContext // needed to save as CSV

    val startTime = System.nanoTime()

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
      print("Search for the correlate song")
      // id of the main song
      val idSong = "spotify:track:5xlWA3V2l7ZiqYF8Ag5EM8"

      // RDD of (pid, trackUri)
      val trackInPlaylistReduce = rddTracksInPlaylist.map(x => (x._1, x._2))

      // filter to keep only the id of playlist that contains the specific song
      val playlistForTrack = trackInPlaylistReduce.filter  { case (_, trackUri) => trackUri == idSong }.map(x => x._1)

      // trasform the collection in RDD to join with trackInPlaylistReduce
      val RDDPlaylistForTrack = playlistForTrack.map((_,null))

      // join
      val trackInSamePlaylists = trackInPlaylistReduce
        .join(RDDPlaylistForTrack)
        .filter(_._2._1 != idSong)
        .map { case (pid, (trackUri, _)) => (pid, trackUri) }

      // result: RDD of (pid, trackUri) with only playlists that contains the specific song
      // and the relatives tracks

      // create the pairs of ((mySong, otherSong), 1) for each playlist
      val rddTrackPairs = trackInSamePlaylists
        .map { case (_, track) => ((idSong, track), 1)}

      // rdd of form ((track1,track2), 1) use to count the occurrences

      // reduce by key to count the occurrences
      val occurrencesCount = rddTrackPairs.reduceByKey(_ + _)

      // take the pair with the highest count
      val mostOccurrencesPair = occurrencesCount
        .reduce((x, y) => if (x._2 > y._2) x else y)

      // Unisci i dettagli della traccia specifica e delle tracce correlate
      val trackDetails = rddTracks.map(x => (x._1, x._2)) // (trackUri, trackName)

      // mostOccurrencesPair to rdd to join
      val mostOccurrencesPairRDD = spark.sparkContext.parallelize(Seq(mostOccurrencesPair))

      val enrichedResults = mostOccurrencesPairRDD
        .map {case ((track1, track2), count) => (track1, (track2, count)) }
        .join(trackDetails) // Unisci il nome della traccia principale
        .map { case (track1, ((track2, count), track1Name)) => (track2, (track1, track1Name, count)) }
        .join(trackDetails) // Unisci il nome delle co-tracce
        .map { case (track2, ((track1, track1Name, count), track2Name)) => (track1, track1Name, track2, track2Name, count) }

      // Salva il risultato
      enrichedResults.coalesce(1).saveAsTextFile(Config.projectDir + "output/result")

    }
    else if (job == "3") {
      // Job Gigi Optimized
    }
    else if (job == "4") {
      // Job Tommi optimized
      print("Search for the correlate song - optimized")
      // id of the main song
      val idSong = "spotify:track:5xlWA3V2l7ZiqYF8Ag5EM8"

      // RDD of (pid, trackUri)
      val trackInPlaylistReduce = rddTracksInPlaylist.map(x => (x._1, x._2))

      // filter to obtain the id of playlists that contains the track
      val playlistForTrack = trackInPlaylistReduce
        .filter { case (_, trackUri) => trackUri == idSong }
        .map(_._1)

      // Broadcast the list of playlists
      val playlistsBroadcast = spark.sparkContext.broadcast(playlistForTrack.collect().toSet)

      // filter to obtain all the songs in playlist that contains the track
      val trackInSamePlaylists = trackInPlaylistReduce
        .filter { case (pid, _) => playlistsBroadcast.value.contains(pid) }
        .filter { case (_, trackUri) => trackUri != idSong }

      // RDD of (pid, trackUri)

      // create the pairs of ((mySong, otherSong), 1) for each playlist
      val rddTrackPairs = trackInSamePlaylists
        .map { case (_, track) => ((idSong, track), 1) }

      // reduce by key to count the occurrences
      val occurrencesCount = rddTrackPairs.reduceByKey(_ + _)

      // take the pair with the highest count
      val mostOccurrencesPair = occurrencesCount
        .max()(Ordering.by(_._2))

      // mostOccurrencesPair to rdd to join
      val mostOccurrencesPairRDD = spark.sparkContext.parallelize(Seq(mostOccurrencesPair))


      // Unisci i dettagli della traccia specifica e delle tracce correlate
      val trackDetails = rddTracks.map(x => (x._1, x._2)) // (trackUri, trackName)

      val partitionedTrackDetails = trackDetails.repartition(1)

      val enrichedResults = mostOccurrencesPairRDD
        .map {case ((track1, track2), count) => (track1, (track2, count)) }
        .join(partitionedTrackDetails) // Unisci il nome della traccia principale
        .map { case (track1, ((track2, count), track1Name)) => (track2, (track1, track1Name, count)) }
        .join(partitionedTrackDetails) // Unisci il nome delle co-tracce
        .map { case (track2, ((track1, track1Name, count), track2Name)) => (track1, track1Name, track2, track2Name, count) }

      // Salva il risultato
      enrichedResults.coalesce(1).saveAsTextFile(Config.projectDir + "output/result")

      /*
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
*/
    }

    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
    println(s"Execution time: $duration seconds")
  }

}
