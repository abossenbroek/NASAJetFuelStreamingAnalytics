/* SimpleApp.scala */
package org.abossenbroek.NASAJetFuelStreamingAnalytics

import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.streaming._

import org.apache.spark.{SparkConf, SparkContext}


object NasaStreaming {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("NASAStreaming")
      .setMaster("local[*]")

    //Create a SparkContext to initialize Spark
    val sc = SparkSession.builder.config(conf).getOrCreate()

    import sc.sql
    import sc.implicits._
    // A batch is created every 30 seconds
    val ssc = new org.apache.spark.streaming.StreamingContext(
      sc.sparkContext,
      org.apache.spark.streaming.Seconds(30))


    val fd001Streamer = new NasaStreamingSource("src/resources/train_FD001.txt", conf)

    // Create the stream
    val stream = ssc.receiverStream(fd001Streamer)

    val display = stream.foreachRDD { rdd =>
      println(
        s"===========\n${rdd.toDebugString}\n=====Len array:${rdd.count()}")
    }

//    // Process RDDs in the batch
    stream.foreachRDD { rdd =>
      {
        val rowRDD = rdd.flatMap(r => identity(r))
        val df = sc.createDataFrame(rowRDD, ReadNasaDataFile.nasaSchema)
        df.show()
//        val list = rdd.map(r => r.toList)
//      println(
//        s"===========\nLen list:${list.count()}")
//        list.foreach(l => println(l))

        // Access the SQLContext and create a table called nasa_streaming we can query
//        val _sqlContext =
//          org.apache.spark.sql.SQLContext.getOrCreate(rdd.sparkContext)
//        _sqlContext
//          .createDataFrame(rdd, fd001Streamer.schema)
//          .show
////          .registerTempTable("fd001_streaming")
      }
    }

//    stream.foreachRDD { (rdd, time) =>
//
//    }

    // Start the stream processing
    ssc.start()

    ssc.awaitTermination()

  }
}
