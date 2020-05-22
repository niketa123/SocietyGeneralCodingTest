package com.codingtest.problem2.spark

import com.codingtest.problem2.spark.Constatnts._

import scala.util.{Failure, Success}

object AdharJob extends App with AdharJobHelper {

  getFilePath match {
    case Success(filePath:String) =>val inputDf = readCsvFile(filePath)
      val  filteredDf = applyFiltersToInputData(inputDf)
      filteredDf
        .coalesce(1)
        .write
        .option(Mode,OverWrite)
        .option(Header,True)
        .csv(OutputFilePath)

    case Failure(exception) => println("Exception while fetching Path")
                                exception.printStackTrace()
  }



}
