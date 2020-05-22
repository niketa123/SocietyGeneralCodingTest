package com.codingtest.Utility

import org.apache.spark.sql.SparkSession

object SparkUtility {

  // Creating Spark session
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

}
