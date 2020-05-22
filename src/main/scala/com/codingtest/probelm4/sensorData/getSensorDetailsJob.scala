package com.codingtest.probelm4.sensorData

import scala.util.{Failure, Success}

object getSensorDetailsJob extends App with SensorDataImpl {

     readInputDataPath match {
    case Success(path) => val inpDf = readInputDataFrame(path)
                          val outputDf = processInputDataFrame(inpDf)
      outputDf.show(false)
    case Failure(exception) => println(s"exception occurred while reading input file path")
    exception.printStackTrace()
  }

}
