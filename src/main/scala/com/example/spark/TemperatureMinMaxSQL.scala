package com.example.spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._

/**
 * Use DataFrame and SQL
 */
object TemperatureMinMaxSQL {

  case class Temperature(month: String, maxTemp: Double, minTemp: Double)

  def main(args: Array[String]) {
    if (args.length < 3) {
      throw new IllegalArgumentException("<inputFile> <outputPath> <prefix> are required");
    }

    // input data path
    val inputPath = args(0);
    // output path
    val outputPath = args(1);
    // prefix filter
    val prefix = args(2);

    // Initialize SparkContext and SQLContext
    val conf = new SparkConf
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    try {
      // Data Structure
      // ----------------
      // 0: yyyy/mm/dd
      // 1: max
      // 7: min
      // ----------------
      val textRDD = sc.textFile(inputPath)

      val temperatureDF = textRDD
        // Handle only lines starts with 201xxx
        .filter(line => line.startsWith(prefix))
        // Split ","
        .map { line =>
          val cols = line.split(",")
          val month = cols(0).split("/")(1)
          // month, max, min
          Temperature(month, cols(1).toDouble, cols(7).toDouble)
        }.toDF
        
        temperatureDF.createOrReplaceTempView("temperature")

        val sqlDF = sqlContext.sql("""
            SELECT month, max(maxTemp), min(minTemp)
            FROM temperature GROUP BY month
        """)
        sqlDF.show()
    } finally {
      sc.stop();
    }
  }
}
