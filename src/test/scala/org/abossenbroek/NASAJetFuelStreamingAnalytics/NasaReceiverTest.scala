import org.abossenbroek.NASAJetFuelStreamingAnalytics.NasaStreamingSource
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class NasaReceiverTest extends FlatSpec with Matchers with BeforeAndAfter {
  private val master: String = "local[*]"
  private val appName: String = "readNasaTrainFile-test"
  private val filePath: String = "src/resources/train_FD001.txt"

  private var sc: SparkSession = _
  private var ssc: StreamingContext = _
  private var conf: SparkConf = _
  private var stream: ReceiverInputDStream[Array[Row]] = _

  before {
    conf = new SparkConf()
      .setAppName("NASAStreaming")
      .setMaster("local[*]")

    //Create a SparkContext to initialize Spark
    sc = SparkSession.builder.config(conf).getOrCreate()

    // A batch is created every 30 seconds
    ssc = new org.apache.spark.streaming.StreamingContext(
      sc.sparkContext,
      org.apache.spark.streaming.Seconds(1))


    val fd001Streamer = new NasaStreamingSource(filePath, conf)

    // Create the stream
    //stream = ssc.receiverStream(fd001Streamer)
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
  }

  "ReadNasaTrainingFile " should " read file to data frame" in {
    //val testFile = ReadNasaDataFile.readFile(filePath, sc)
    //assert(testFile.rdd.count() == 20631)
  }

}
