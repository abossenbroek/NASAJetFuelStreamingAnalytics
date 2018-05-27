package org.abossenbroek.NASAJetFuelStreamingAnalytics

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level

import scala.collection.JavaConverters._

class NasaStreamingSource(pathName: String)
    extends org.apache.spark.streaming.receiver.Receiver[Array[Row]](
      org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_2)
    with Logging {
  logger.info(s"Constructing NasaStreamingSource for pathName: $pathName")
  var schema: StructType = _

  /** Start the thread that simulates receiving data */
  def onStart(): Unit = {
    logger.info(
      s"Starting thread for NasaStreamingSource for pathName: $pathName")
    new Thread("Nasa Source") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop(): Unit = {}

  /* Periodically feed a Row of information with the current time in last
   * column */
  private def receive(): Unit = {
    var counter = 1
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("NASAStreaming")
      .getOrCreate()
    import spark.implicits._

    // Create the FileInputDStream on the directory
    logger.info(s"Opening: $pathName")
    val source = ReadNasaDataFile.readFile(pathName, spark)
    //schema = source.schema.add(StructField(s"Time", LongType, nullable = false))
    schema = source.schema
    val sourceRowCount: Long = source.rdd.count()
    logger.info(s"Found sourceRowCount: $sourceRowCount")

    def sourceLine(count: Long): Int = (sourceRowCount % count).toInt

    while (!isStopped()) {
      logger.info(
        s"For streamer with path $pathName current counter is at $counter")

      //val row = source.take(sourceLine(counter))//.drop(sourceLine(counter) - 1)
      val row = source.take(counter).drop(sourceLine(counter) - 1)

      store(Iterator(row))
      counter += 1
      Thread.sleep(5000)
    }
  }
}
