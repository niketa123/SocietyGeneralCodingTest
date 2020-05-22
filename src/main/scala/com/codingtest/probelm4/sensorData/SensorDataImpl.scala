package com.codingtest.probelm4.sensorData

import com.codingtest.Utility.SparkUtility
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead, when}
import org.apache.spark.sql.types._

import scala.util.Try

trait SensorDataImpl {

  def readInputDataPath():Try[String] ={
    Try( getClass.getResource("/input/senosrData.csv").toString)
  }

  def readInputDataFrame(path:String):DataFrame ={
    SparkUtility.spark
      .read
      .option("Header","true")
      .schema(getInputSchema)
      .csv(path)
  }

  def getInputSchema ={
    StructType(Seq(StructField("Sensor",StringType,true),
      StructField("Mnemonic",StringType,true),
      StructField("data",IntegerType,true),
      StructField("timestamp",LongType,true)))
  }

  /**
   * Finds start and end DataFrame whenever data is changed from 1-0 or 0-1
   * First lag of data is found and named as PrevData
   * If data and prev Data is same (i.e 0,0 or 1,1) then both start and end Time are null
   * other wise startTime = lag(timestamp) and
   *            endTime = lead(timestamp)
   * @param inputDf InputDataFrame
   * @return
   */
  def processInputDataFrame(inputDf:DataFrame):DataFrame = {
    // Defining Window
    val win = Window.partitionBy("Mnemonic").orderBy("timestamp")
    inputDf
      .withColumn("prevData",lag("data",1).over(win))
      .withColumn("startTime",when(
        (col("data") === col("prevData") || col("prevData").isNull),
        "null").otherwise(lag("timestamp",1).over(win)))
      .withColumn("endTime",when(
        (col("data") === col("prevData") || col("prevData").isNull),
        "null").otherwise(lead("timestamp",1).over(win)))
      .select("Sensor","Mnemonic","data","startTime","endTime")

  }

}
