package org.abossenbroek.NASAJetFuelStreamingAnalytics

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}

class ReadNasaDataFile(fileName: String, sc: SparkSession) {
  val userSchema: StructType = StructType(List(StructField("unitNumber", IntegerType, true))
    ++ List(StructField("timeCycles", IntegerType, true))
    ++ (for (i <- 1 to 3) yield StructField(s"operationalSetting_$i", FloatType, true)).toList
    ++ (for (i <- 1 to 26) yield StructField(s"sensorMeas_$i", FloatType, true)).toList)

  val df: sql.DataFrame = sc.read
      .option("sep", " ")
      .schema(userSchema)      // Specify schema of the csv files
      .csv(fileName)    // Equivalent to format("csv").load("/path/to/directory"))
}
