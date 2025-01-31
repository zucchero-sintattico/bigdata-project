import org.apache.spark.HashPartitioner
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.{Commons, Config}

import scala.collection.mutable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, DoubleType}

object Job {


  val path_to_datasets = "datasets/processed/"

  private val path_tracks = path_to_datasets + "tracks.csv"
  private val path_playlists = path_to_datasets + "playlists.csv"
  private val path_tracks_in_playlist = path_to_datasets + "tracks_in_playlist.csv"
  private val path_artists = path_to_datasets + "artists.csv"

  private val path_to_avg_song_per_artist = "output/avg_song_per_artist/"

  val schema = StructType(List(StructField("result", DoubleType, nullable = false)))


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
    val job = "3"
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
      val rddTracksInPlaylistTracks = rddTracksInPlaylist.keyBy({
          case (_, t_uri, _) => t_uri
        })
        .join(rddTracks.keyBy({ case (t_uri, _, _, _, _, _) => t_uri }
        ))
        // take all fields of the track, and the playlist PID
        .map(x => (x._2._1._1, x._2._2._2, x._2._2._3, x._2._2._4, x._2._2._5, x._2._2._6))
      val rddTracksInPlaylistTracksArtists = rddTracksInPlaylistTracks.keyBy(_._4)
        .join(rddArtists.keyBy(_._1))
        // keep all the fields of the track, and the playlist PID and the artist name
        .map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, x._2._2._2))

      // (PID, track_name, duration_ms, artist_uri, album_uri, album_name, artist_name)
      val pidArtistTrack = rddTracksInPlaylistTracksArtists.map(x => ((x._1, x._4), 1))
      val artistTrackCount = pidArtistTrack
        .reduceByKey(_ + _)

      val pidToArtistTracks = artistTrackCount.map(x => (x._1._1, x._2))
      // (PID, num_tracks)
      val averageSongsPerArtist = pidToArtistTracks
        .groupByKey() // Raggruppa tutte le playlist
        .mapValues { counts =>
          val totalArtists = counts.size
          val totalTracks = counts.sum
          totalTracks.toDouble / totalArtists
        }

      // Calcolo della media complessiva
      val totalPlaylists = averageSongsPerArtist.count()
      val sumOfAverages = averageSongsPerArtist.map(_._2).sum()

      val result = sumOfAverages / totalPlaylists // Media complessiva

      val rowRDD = spark.sparkContext.parallelize(Seq(Row(result)))
      val resultDF = spark.createDataFrame(rowRDD, schema)

      resultDF.write.format("csv").mode(SaveMode.Overwrite).save(Config.projectDir + "output/result")
    }
    else if (job == "2") {
      // Job Tommi
      // id of the main song
      val idSong = "spotify:track:5xlWA3V2l7ZiqYF8Ag5EM8"

      // RDD of (pid, trackUri)
      val trackInPlaylistReduce = rddTracksInPlaylist.map(x => (x._1, x._2))

      // filter to keep only the id of playlist that contains the specific song
      val playlistForTrack = trackInPlaylistReduce.filter { case (_, trackUri) => trackUri == idSong }.map(x => x._1)


      // trasform the collection in RDD to join with trackInPlaylistReduce
      val RDDPlaylistForTrack = playlistForTrack.map((_, null))

      // join
      val trackInSamePlaylists = trackInPlaylistReduce
        .join(RDDPlaylistForTrack)
        .filter(_._2._1 != idSong)
        .map { case (pid, (trackUri, _)) => (pid, trackUri) }

      // result: RDD of (pid, trackUri) with only playlists that contains the specific song
      // and the relatives tracks

      // create the pairs of ((mySong, otherSong), 1) for each playlist
      val rddTrackPairs = trackInSamePlaylists
        .map { case (_, track) => ((idSong, track), 1) }

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
        .map { case ((track1, track2), count) => (track1, (track2, count)) }
        .join(trackDetails) // Unisci il nome della traccia principale
        .map { case (track1, ((track2, count), track1Name)) => (track2, (track1, track1Name, count)) }
        .join(trackDetails) // Unisci il nome delle co-tracce
        .map { case (track2, ((track1, track1Name, count), track2Name)) => (track1, track1Name, track2, track2Name, count) }

      // Salva il risultato
      enrichedResults.coalesce(1).saveAsTextFile(Config.projectDir + "output/result")

    }
    else if (job == "3") {
      // Job Gigi Optimized
      import org.apache.spark.HashPartitioner

      // Numero di partizioni, dipende dalle risorse del cluster
      val numPartitions = spark.sparkContext.defaultParallelism
      val partitioner = new HashPartitioner(numPartitions)

      // Preparazione dei dati per il broadcast
      val broadcastTracks = spark.sparkContext.broadcast(
        rddTracks.map {
          case (track_uri, _, _, artist_uri, _, _) => (track_uri, artist_uri)
        }.collectAsMap() // Converti in una mappa per lookup efficiente
      )

      val broadcastArtists = spark.sparkContext.broadcast(
        rddArtists.map {
          case (artist_uri, artist_name) => (artist_uri, artist_name)
        }.collectAsMap() // Converti in una mappa per lookup efficiente
      )

      // Trasformazione principale con broadcast join
      val rddPidArtistNTracks = rddTracksInPlaylist
        .mapPartitions { iter =>
          val tracksMap = broadcastTracks.value // Accesso alla mappa broadcast
          val artistsMap = broadcastArtists.value // Accesso alla mappa broadcast

          iter.flatMap {
            case (pid, track_uri, _) =>
              tracksMap.get(track_uri) match {
                case Some(artist_uri) =>
                  artistsMap.get(artist_uri) match {
                    case Some(_) => Some(((pid, artist_uri), 1)) // Solo se l'artista esiste
                    case None => None
                  }
                case None => None
              }
          }
        }

      // Partizionamento e caching
      val rddPidArtistNTracksPartitioned = rddPidArtistNTracks
        .partitionBy(partitioner)

      // Calcolo del numero totale di brani per ogni artista in ogni playlist
      val artistTrackCount = rddPidArtistNTracksPartitioned.reduceByKey(_ + _) // (PID, artist_uri) -> conteggio

      // Calcolo della somma e del conteggio per ogni playlist
      val pidToArtistTracks = artistTrackCount.map(x => (x._1._1, x._2)) // PID -> conteggio

      val averageSongsPerArtist = pidToArtistTracks.aggregateByKey((0, 0))(
        // Combina localmente (somma parziale e conteggio)
        (acc, value) => (acc._1 + value, acc._2 + 1),
        // Combina globalmente i risultati delle partizioni
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      ).mapValues { case (totalTracks, totalArtists) =>
        totalTracks.toDouble / totalArtists
      }

      // Calcolo della media complessiva
      val (sumOfAverages, totalPlaylists) = averageSongsPerArtist.mapPartitions(iter => {
        var sum = 0.0
        var count = 0L
        iter.foreach {
          case (_, avg) =>
            sum += avg
            count += 1
        }
        Iterator((sum, count))
      }).reduce {
        case ((sum1, count1), (sum2, count2)) =>
          (sum1 + sum2, count1 + count2)
      }
      val result = sumOfAverages / totalPlaylists

      // save on output directory
      val rowRDD = spark.sparkContext.parallelize(Seq(Row(result)))
      val resultDF = spark.createDataFrame(rowRDD, schema)
      resultDF.write.format("csv").mode(SaveMode.Overwrite).save(Commons.getDatasetPath(writeMode, path_to_avg_song_per_artist))
    }
    else if (job == "4") {
      // Job Tommi optimized

      // id of the main song
      val idSong = "spotify:track:5xlWA3V2l7ZiqYF8Ag5EM8"

      // RDD of (pid, trackUri)
      val trackInPlaylistReduce = rddTracksInPlaylist.map(x => (x._1, x._2))

      // filter to obtain the id of playlists that contains the track
      val playlistForTrack = trackInPlaylistReduce
        .filter { case (_, trackUri) => trackUri == idSong }
        .map(_._1)
        .distinct()

      // Broadcast the list of playlists
      val playlistsBroadcast = spark.sparkContext.broadcast(playlistForTrack.collect().toSet)


      // filter to obtain all the songs in playlist that contains the track
      val trackInSamePlaylists = trackInPlaylistReduce
        .filter { case (pid, _) => playlistsBroadcast.value.contains(pid) }
        .filter { case (_, trackUri) => trackUri != idSong }

      // RDD of (pid, trackUri)

      // create the pairs of ((mySong, otherSong), 1) for each playlist and reduce by key to count the occurrences
      val occurrencesCount = trackInSamePlaylists
        .map { case (_, track) => ((idSong, track), 1) }
        .reduceByKey(_ + _)

      // reduce by key to count the occurrences
      //val occurrencesCount = rddTrackPairs.reduceByKey(_ + _)

      // take the pair with the highest count
      val mostOccurrencesPair = occurrencesCount
        .max()(Ordering.by(_._2))

      // mostOccurrencesPair to rdd to join
      val mostOccurrencesPairRDD = spark.sparkContext.parallelize(Seq(mostOccurrencesPair))

      // Unisci i dettagli della traccia specifica e delle tracce correlate
      val trackDetails = rddTracks.map(x => (x._1, x._2)) // (trackUri, trackName)

      // num of partitioner default
      val partitionedTrackDetails = trackDetails.partitionBy(new HashPartitioner(trackDetails.getNumPartitions))

      val enrichedResults = mostOccurrencesPairRDD
        .map { case ((track1, track2), count) => (track1, (track2, count)) }
        .join(partitionedTrackDetails) // Unisci il nome della traccia principale
        .map { case (track1, ((track2, count), track1Name)) => (track2, (track1, track1Name, count)) }
        .join(partitionedTrackDetails) // Unisci il nome delle co-tracce
        .map { case (track2, ((track1, track1Name, count), track2Name)) => (track1, track1Name, track2, track2Name, count) }

      // Salva il risultato
      enrichedResults.coalesce(1).saveAsTextFile(Config.projectDir + "output/result")

    }

    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
    println(s"Execution time: $duration seconds")
  }

}
