package com.codingtest.problem2.scala
import com.codingtest.problem2.spark.AadharEntity2

import scala.io.Source
import scala.util.Try

trait Helper {

  def getFilePath =  Try(getClass.getResource("/input/problem2/auth.csv").getPath)

  /**
   * Reads Adhar infor file and split by ","
   * Selects aua,sa and res_state_name based on index
   * Since Aua and Sa contains string and numeric values hence use Either[String,Int]
   * * @param filePath
   * @return
   */
  def readCsvFile(filePath:String) ={

    println("FilePath is "+filePath)
   val data = Source.fromFile(filePath).getLines()
    data.drop(1)
        .map(line =>line.split(",")).map(row => AadharEntity2(
      try {
        Right(row(2).toInt)
      } catch {
        case e: Exception =>
          Left(row(2))
      }
      ,
      try {
        Right(row(3).toInt)
      } catch {
        case e: Exception =>
          Left(row(3))
      },row(128) ))
  }

  /**
   * Filtered for State other than Delhi
   * Only Numeric Sa values and which Right value of Either
   * Aua >65000
   * @param data
   * @return
   */
  def applyFilters(data :Iterator[AadharEntity2]) ={

   data.filter( row => !row.res_state_name.equalsIgnoreCase("delhi")
                       && row.sa.isRight)
      .filter(row => row.aua match {
        case Left(a) => false
        case Right(aua) => if(aua > 65000) true else false
      })
  }



}
