/* SimpleApp.scala */
package org.abossenbroek.NASAJetFuelStreamingAnalytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds

object NasaStreaming extends App {
  override def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("NASAStreaming")
      .setMaster("local[*]")

    //Create a SparkContext to initialize Spark
    val sc = SparkSession.builder.config(conf).getOrCreate()

    // A batch is created every 30 seconds
    val ssc = new org.apache.spark.streaming.StreamingContext(
      sc.sparkContext,
      org.apache.spark.streaming.Seconds(10))

    val fd001Streamer =
      new NasaStreamingSource("src/resources/train_FD001.txt", conf)

    // Create the stream
    val stream = ssc.receiverStream(fd001Streamer)

    val display: Unit = stream.foreachRDD { (rdd, time) =>
      println(
        s"===========\n${rdd.toDebugString}\n=====Len array:${rdd.count()}")
    }

//    val keyedDstream = stream.transform(rdd => rdd.distinct).map(e => (e, 1))
//    val windowed = keyedDstream.reduceByKeyAndWindow((x: Int, y: Int) => x + y,
//                                                     Seconds(30),
//                                                     Seconds(10))
//    // join the windowed count with the initially keyed dstream
//    val joined = keyedDstream.join(windowed)
//    // the unique keys though the window are those with a running count of 1 (only seen in the current interval)
//    val uniquesThroughWindow = joined.transform { rdd =>
//      rdd.collect { case (k, (current, prev)) if (prev == 1) => k }
//    }
//
//    uniquesThroughWindow.foreachRDD(rdd => {
//      val rowRDD = rdd.flatMap(r => identity(r))
//      val df = sc.createDataFrame(rowRDD, ReadNasaDataFile.nasaSchema)
//      df.show()
//    })

    // Process RDDs in the batch
    stream.foreachRDD { rdd =>
      {
        var rowRDD = rdd.flatMap(r => identity(r))
        rowRDD = rowRDD.distinct
        val df = sc.createDataFrame(rowRDD, ReadNasaDataFile.nasaSchema)
        df.show()
      }
    }

    // Start the stream processing
    ssc.start()

    ssc.awaitTermination()

  }
}
