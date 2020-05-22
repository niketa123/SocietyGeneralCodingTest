package com.codingtest.problem1

import com.codingtest.Utility.SparkUtility
import com.codingtest.problem1.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.types.{FloatType, IntegerType}

trait ReportGeneratorHelper {

  /**
   * Reads excel files and create sql Dataframe
   * @param filePath : path of excel file
   * @param sheetName
   * @return Dataframe
   */
  def readExcelFile(filePath:String,
                    sheetName:Option[String]) ={
         sheetName match {
              case Some(sheet) => SparkUtility
                  .spark
                .read
                .format(ExcelSource)
                .option(UseHeader,True)
                .option(SheetNameKey,sheetName.get)
               .option(TreatEmptyValuesAsNulls,True)
               .option(InferSchema,False)
              .option(AddColorColumns,False)
               .option(Location,filePath)
               .load()
                  .select(Stabr,Area_name,Rural_urban_Continuum_Code_2013,Urban_Influence_Code_2003,
                    PCTPOVALL_2018,PCTPOV017_2018)
              case None =>
                      SparkUtility
                       .spark
                        .read
                       .format(ExcelSource)
                       .option(UseHeader,True)
                        .option(TreatEmptyValuesAsNulls,True)
                         .option(InferSchema,False)
                        .option(AddColorColumns,False)
                       .option(Location,filePath)
                        .load()
                        .withColumnRenamed(Postal_Abbreviation,Stabr)
                        .withColumnRenamed(Capital_Name,State)
   }

  }

  /**
   * Changes column types to required types as Input dataFrame default has String Type
   * @param inputDf
   * @return
   */
  def changeColumnTypes(inputDf:DataFrame) ={
    inputDf.withColumn(Rural_urban_Continuum_Code_2013,col(Rural_urban_Continuum_Code_2013).cast(IntegerType))
           .withColumn(Urban_Influence_Code_2003,col(Urban_Influence_Code_2003).cast(IntegerType))
           .withColumn(PCTPOV017_2018,col(PCTPOV017_2018).cast(FloatType))
          .withColumn(PCTPOVALL_2018,col(PCTPOVALL_2018).cast(FloatType))

  }

  /**
   * Applies below filter conditions on input dataFrame
   * Filters for even Rural_urban_Continuum_Code_2013
   * Filters for odd Urban_Influence_Code_2003
   * Appends Suffix Area Name with State Code
   * @param inputDf PovertyReport as Sql DataFrame
   * @return filtered DataFrame
   */

  def applyFiltersToInputData(inputDf:DataFrame): DataFrame ={

    inputDf.filter(col(Urban_Influence_Code_2003) % 2 =!= 0
                   && col(Rural_urban_Continuum_Code_2013) % 2 === 0)
           .withColumn(Area_name,concat(col(Area_name),lit(Space),col(Stabr)))
  }

  /**
   * Calculates EstimatedPercentOfPplOlderThan17 by following step
   *  col(PCTPOVALL_2018) - col(PCTPOV017_2018)
   *  col PCTPOVALL_2018  contains of all age group data and  col PCTPOV017_2018
   *  contains data of 0-17 age hence subtracting these two columns would give data of >17 age
   * @param inputDf Filtered poverty report as DataFrame
   * @return DataFrame with EstimatedPercentOfPplOlderThan17
   */
  def findEstimatedPercentOfPplOlderThan17(inputDf:DataFrame): DataFrame ={

    inputDf
      .withColumn(POV_elder_than17_2018,
        col(PCTPOVALL_2018)-col(PCTPOV017_2018))
  }

  /**
   * Joins report and states on State abbrivation to get full name of State
   * @param inputDf  Filtered input DataFrame
   * @param statesDf DataFrame having States
   * @return
   */

  def getStateName(inputDf:DataFrame,
                   statesDf:DataFrame): DataFrame ={
   inputDf.join(statesDf,Seq(Stabr),Inner)
         .select(State,Area_name,Rural_urban_Continuum_Code_2013,
           Urban_Influence_Code_2003,POV_elder_than17_2018)
  }
}
