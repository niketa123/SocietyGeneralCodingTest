package com.codingtest.problem1

import com.codingtest.problem1.Constants._

object ReportGenerator extends App with ReportGeneratorHelper {

  val inputFilePath = getClass.getResource("/input/PovertyEstimates.xls").getPath

  val statesNameFilePath = getClass.getResource("/input/StatesName.xlsx").getPath

  // creates inputDf from poverty report excel file
  val inputDf = readExcelFile(inputFilePath,Some(SheetName))

  // creates statesInfoDf from StatesName excel
  val statesInfoDf = readExcelFile(statesNameFilePath,None)

  // Changes required column types
  val typedInputDf = changeColumnTypes(inputDf)

  // Applies filters as mention in documnet
  val filteredDf = applyFiltersToInputData(typedInputDf)

  // Calculates EstimatedPercentOfPplOlderThan17
  val pplOlderThan17InfoDf                = findEstimatedPercentOfPplOlderThan17(filteredDf)

  // Performs join to get State full Name
  val outputDf = getStateName(pplOlderThan17InfoDf,statesInfoDf)

  outputDf
      .coalesce(1)
    .write
    .option(Mode,OverWrite)
    .option(Header,True)
    .csv(OutputFilePath)

}
