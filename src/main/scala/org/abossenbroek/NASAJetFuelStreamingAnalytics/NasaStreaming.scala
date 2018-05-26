/* SimpleApp.scala */
package org.abossenbroek.NASAJetFuelStreamingAnalytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

object NasaStreaming {
  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val sc = SparkSession.builder
      .appName("NASAStreaming")
      .master("local[*]")
      .getOrCreate()

    // Create the FileInputDStream on the directory
    val fd001 = ReadNasaDataFile.readFile("src/resources/train_FD001.txt", sc)
    fd001.show()
//    // A batch is created every 30 seconds
//    val ssc = new org.apache.spark.streaming.StreamingContext(spark.sparkContext, org.apache.spark.streaming.Seconds(30))
//
//    // Set the active SQLContext so that we can access it statically within the foreachRDD
//    org.apache.spark.sql.SQLContext.setActive(spark.sqlContext)
//
//    // Create the stream
//    val nasaSource = new NasaSource(nasaFile)
//    val stream = ssc.receiverStream(nasaSource)
//
//    // Process RDDs in the batch
//    stream.foreachRDD { rdd =>
//      // Access the SQLContext and create a table called nasa_streaming we can query
//      val _sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(rdd.sparkContext)
//      _sqlContext.createDataFrame(rdd, nasaSource.schema).registerTempTable("nasa_streaming")
//    }
//
//    // Start the stream processing
//    ssc.start()

  }
}
