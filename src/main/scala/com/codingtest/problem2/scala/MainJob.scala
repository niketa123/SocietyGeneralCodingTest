package com.codingtest.problem2.scala

import scala.util.{Failure, Success, Try}

object MainJob extends App with Helper {

  getFilePath match {
    case Success(filepath) =>
      val data = readCsvFile(filepath)
      val filteredData = applyFilters(data)

      filteredData.foreach(println)

    case Failure(exception) => println("Exception while retreiving auth.csv file")
  }

}
