import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.{Commons, Config}

import scala.collection.mutable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, DoubleType}

object Job {


  val path_to_datasets = "/datasets/processed/"

  private val path_tracks = path_to_datasets + "tracks.csv"
  private val path_playlists = path_to_datasets + "playlists.csv"
  private val path_tracks_in_playlist = path_to_datasets + "tracks_in_playlist.csv"
  private val path_artists = path_to_datasets + "artists.csv"

  val schema = StructType(List(StructField("result", DoubleType, nullable = false)))


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

      // RDD of (pid, trackUri)
      val playlistTracks = rddTracksInPlaylist.map(x => (x._1, x._2))

      // auto-join on track_in_playlist to get all song pairs in the same playlist
      val rddTrackPairs = playlistTracks.join(playlistTracks)
        .filter { case (_, (track1, track2)) => track1 != track2 }
        .map { case (_, (track1, track2)) => ((track1, track2), 1) }

      // => rdd di forma ((track1,track2), 1) use to count the occurrences

      val occurrencesCount = rddTrackPairs.reduceByKey(_ + _)

      // take the pair with the highest value for each track
      val mostOccurrencesPair = occurrencesCount
        .map { case ((track1, track2), count) => (track1, (track2, count)) }
        .reduceByKey { case ((track2, count1), (track3, count2)) => if (count1 > count2) (track2, count1) else (track3, count2) }

      // join on rddTracks for track names
      val trackDetails = rddTracks.map(x => (x._1, x._2))
      val firstResult = mostOccurrencesPair.join(trackDetails)
        .map { case (track1, ((track2, count), track1Name)) => (track1, track1Name, track2, count) }

      val result = firstResult
        .map { case (track1, name, track2, count) => (track2, (track1, name, count)) }
        .join(trackDetails)
        .map { case (track2, ((track1, name, count), track2Name)) => (track1, name, track2, track2Name, count) }

      result.saveAsTextFile(Config.projectDir + "output/result")

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
      resultDF.write.format("csv").mode(SaveMode.Overwrite).save(Config.projectDir + "output/result")
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
        .aggregateByKey(mutable.Map[String, Int]())( // ogni chiave (traccia) viene inizializzata con una mappa che terrà il count delle sue co-tracce
          (acc, coTrack) => {
            acc(coTrack) = acc.getOrElse(coTrack, 0) + 1
            acc
          }, // per ogni chiave vengono iterati i valori associati (coTrack) e incrementato il count
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
