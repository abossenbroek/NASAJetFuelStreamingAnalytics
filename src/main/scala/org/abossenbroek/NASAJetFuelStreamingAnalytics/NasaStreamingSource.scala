package org.abossenbroek.NASAJetFuelStreamingAnalytics

import org.apache.spark.sql.Row
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.SparkConf
import org.apache.spark.streaming.receiver.Receiver


class NasaStreamingSource(pathName: String, conf: SparkConf)
    extends Receiver[Array[Row]](
      org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_2)
    with Logging {
  logger.info(s"Constructing NasaStreamingSource for pathName: $pathName")

  def sparkConfig: SparkConf = conf

  /** Start the thread that simulates receiving data */
  def onStart(): Unit = {
    new NasaReceiver(pathName, this).start()
  }

  def onStop(): Unit = {}
}
