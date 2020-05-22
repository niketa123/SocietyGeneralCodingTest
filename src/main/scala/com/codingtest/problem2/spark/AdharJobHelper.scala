package com.codingtest.problem2.spark

import com.codingtest.Utility.SparkUtility
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StructType}
import com.codingtest.problem2.spark.Constatnts._

import scala.util.Try

trait AdharJobHelper extends Serializable {

  def getFilePath =  Try(getClass.getResource("/input/problem2/auth.csv").getPath)

  def readCsvFile(filePath:String) ={
    SparkUtility
      .spark
      .read
      .option(Header,"true")
      .csv(filePath)
      .select(Aua,Sa,Res_state_name)

  }


  /**
   * Since Aua contains string values hence before applying filter, only numirc values are filtered out using regex
   * Filter for Aua > 65000
   * States other than Delhi
   * Only Numeric Sa
   * @param inputDf
   * @return Dataframe
   */

  def applyFiltersToInputData(inputDf:DataFrame): DataFrame = {

    inputDf
        .filter(col(Aua) rlike "^[0-9]*$")
        .withColumn(Aua,col(Aua).cast(DoubleType))
        .filter(col(Aua) > 65000 && col(Res_state_name) =!= "Delhi")
        .filter(col(Sa) rlike "^[0-9]*$")

  }



}
