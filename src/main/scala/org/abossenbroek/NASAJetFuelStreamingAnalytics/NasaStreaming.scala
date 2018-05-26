/* SimpleApp.scala */
package org.abossenbroek.NASAJetFuelStreamingAnalytics

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.streaming._

object NasaStreaming {
  def main(args: Array[String]) {
    //Create a SparkContext to initialize Spark
    val spark = SparkSession
      .builder
      .appName("NASAStreaming")
      .config("spark.driver.host", "SFRMAC-40167-1")
      .master("local[*]")
      .getOrCreate()

    // Create the FileInputDStream on the directory
    val userSchema = StructType((for (i <- 0 to 27) yield StructField(s"V$i", FloatType, true)).toList)
    var nasaFile = spark.read
      .option("sep", " ")
      .schema(userSchema)      // Specify schema of the csv files
      .csv("src/main/resources/train_FD001.txt")    // Equivalent to format("csv").load("/path/to/directory"))
    nasaFile = nasaFile.drop("V26", "V27")

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