package com.codingtest.problem3

import Constants._

object Main extends App with InternationalBaseLineHelper {

  //Process InputFiles for various crop type
  val finalOutputDf = processInputFile

  // WritesFinal O/P
  finalOutputDf
    .write
    .option(Header,true)
    .csv(OutputFilePath)

}
