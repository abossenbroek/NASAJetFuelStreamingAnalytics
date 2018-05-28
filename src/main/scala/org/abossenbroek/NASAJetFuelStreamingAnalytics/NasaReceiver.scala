package org.abossenbroek.NASAJetFuelStreamingAnalytics

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession

class NasaReceiver(val pathName: String, val recv: NasaStreamingSource)
    extends Thread
    with Logging {
  private val sc = SparkSession.builder.config(recv.sparkConfig).getOrCreate()
  private val source = ReadNasaDataFile.readFile(pathName, sc)
  private val sourceRowCount: Long = source.rdd.count()
  private def sourceLine(count: Long): Int = (sourceRowCount % count).toInt

  logger.info( s"Start NasaReceiver for pathName: $pathName")

  private def receive(): Unit = {
    var counter = 1
    logger.info( s"Start receiving NasaReceiver for pathName: $pathName")
    // Create the FileInputDStream on the directory
    while (!recv.isStopped()) {
      val row = source.take(counter).drop(sourceLine(counter) - 1)
      recv.store(Iterator(row))
      counter += 1
      Thread.sleep(5000)
    }
  }

  override def run() {
    receive()
  }
}
