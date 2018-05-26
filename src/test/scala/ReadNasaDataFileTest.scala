import org.abossenbroek.NASAJetFuelStreamingAnalytics.ReadNasaDataFile
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class ReadNasaDataFileTest extends FlatSpec with Matchers with BeforeAndAfter {
  private val master: String = "local[*]"
  private val appName: String = "readNasaTrainFile-test"
  private val filePath: String = "src/resources/train_FD001.txt"

  private var sc: SparkSession = _

  before {
    sc = SparkSession
      .builder
      .appName(appName)
      .master(master)
      .getOrCreate()
  }

  "ReadNasaTrainingFile " should " read file to data frame" in {
    val testFile = ReadNasaDataFile.readFile(filePath, sc)
    assert(testFile.rdd.count() == 20631)
  }

}
