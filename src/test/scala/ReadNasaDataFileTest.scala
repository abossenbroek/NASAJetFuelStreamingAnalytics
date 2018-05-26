import org.abossenbroek.NASAJetFuelStreamingAnalytics.ReadNasaDataFile
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

object ReadNasaDataFileTest extends FlatSpec with Matchers with BeforeAndAfter {
  private val master: String = "local[*]"
  private val appName: String = "readNasaTrainFile-test"
  private val filePath: String = "src/main/resources/train_FD001.txt"

  private var sc: SparkSession = _

  before {
    sc = SparkSession
      .builder
      .appName(appName)
      .master(master)
      .getOrCreate()
  }

  "ReadNasaTrainingFile " should " read file to data frame" in {
    val testFile = new ReadNasaDataFile(filePath, sc)
    assert(testFile.df.count() == 20631)
  }

}
