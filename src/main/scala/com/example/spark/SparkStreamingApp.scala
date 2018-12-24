package com.example.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

/**
 * Streaming App
 */
object SparkStreamingApp {

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
    
    // Initialize SparkContext and SQLContext
    val conf = new SparkConf().setAppName("StreamingApp")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))

    var lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" ")).filter(_.nonEmpty)
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
    wordCounts.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}
