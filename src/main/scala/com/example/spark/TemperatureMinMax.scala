package com.example.spark

import org.apache.spark.{ SparkConf, SparkContext }
import scala.math;

object TemperatureMinMax {
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

    // Initialize SparkContext
    val conf = new SparkConf
    val sc = new SparkContext(conf)

    try {
      // Data Structure
      // ----------------
      // 0: yyyy/mm/dd
      // 1: max
      // 7: min
      // ----------------
      val textRDD = sc.textFile(inputPath)

      textRDD
        // Handle only lines starts with 201xxx
        .filter(line => line.startsWith(prefix))
        // Split ","
        .map(line => {
          val cols = line.split(",")
          val month = cols(0).split("/")(1)
          // month, max, min
          (month, (cols(1).toDouble, cols(7).toDouble))
        })
        // Calculates max or min by month.
        .reduceByKey((prevPair, pair) => {
          val (ptmax, ptmin) = prevPair
          val (tmax, tmin) = pair
          (math.max(ptmax, tmax), math.min(ptmin, tmin))
        })
        .saveAsTextFile(outputPath)
    } finally {
      sc.stop();
    }
  }
}
