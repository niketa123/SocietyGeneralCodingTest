package com.codingtest.problem3

import org.apache.spark

import scala.util.{Failure, Success, Try}
import Constants._
import com.codingtest.Utility.SparkUtility
import org.apache.commons.codec.Encoder
import org.apache.poi.ss.formula.functions.T
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

trait InternationalBaseLineHelper {

  def processInputFile:DataFrame ={

    val usAndWorldBarleyDfs = readInputByType(BarleyType)
    val usBarleyContributionDf = findUsContribution(usAndWorldBarleyDfs,UsBarleyContribution)

    val usAndWorldBeefDf= readInputByType(BeefType)
    val usBeefContributionDf = findUsContribution(usAndWorldBeefDf,UsBeefContribution)

    val usAndWorldCornDf= readInputByType(CornType)
    val usCornContributionDf = findUsContribution(usAndWorldCornDf,UsCornContribution)

    val usAndWorldCottonDf = readInputByType(CottonType)
    val usCottonContributionDf = findUsContribution(usAndWorldCottonDf,UsCottonContribution)

    val usAndWorldPorkDf = readInputByType(PorkType)
    val usPorkContributionDf = findUsContribution(usAndWorldPorkDf,UsPorkContribution)

    val usAndWorldPoultryDf = readInputByType(PoultryType)
    val usPoultryContributionDf = findUsContribution(usAndWorldPoultryDf,UsPoultryContribution)

    val usAndWorldRiceDf = readInputByType(RiceType)
    val usRiceContributionDf = findUsContribution(usAndWorldRiceDf,UsRiceContribution)

    val usAndWorldSorghumDf = readInputByType(SorghumType)
    val usSorghumContributionDf = findUsContribution(usAndWorldSorghumDf,UsSorghumContribution)

    val usAndWorldSoybeanDf = readInputByType(SoybeanType)
    val usSoybeanContributionDf = findUsContribution(usAndWorldSoybeanDf,UsSoybeanContribution)

    val usAndWorldSoybeanMealDf = readInputByType(SoybeanMealType)
    val usSoybeanMealContributionDf = findUsContribution(usAndWorldSoybeanMealDf,UsSoybeanMealContribution)


    val usAndWorldSoybeanOilDf = readInputByType(SoybeanOilType)
    val usSoybeanOilContributionDf = findUsContribution(usAndWorldSoybeanOilDf,UsWheatContribution)


    val usAndWorldWheatDf = readInputByType(WheatType)
    val usWheatContributionDf = findUsContribution(usAndWorldWheatDf,UsSoybeanOilContribution)

    usBarleyContributionDf.join(usBeefContributionDf,Seq(Year),InnerJoin)
      .join(usCornContributionDf,Seq(Year),InnerJoin)
      .join(usCottonContributionDf,Seq(Year),InnerJoin)
      .join(usPorkContributionDf,Seq(Year),InnerJoin)
      .join(usPoultryContributionDf,Seq(Year),InnerJoin)
      .join(usRiceContributionDf,Seq(Year),InnerJoin)
      .join(usSorghumContributionDf,Seq(Year),InnerJoin)
      .join(usSoybeanContributionDf,Seq(Year),InnerJoin)
      .join(usSoybeanMealContributionDf,Seq(Year),InnerJoin)
      .join(usSoybeanOilContributionDf,Seq(Year),InnerJoin)
      .join(usWheatContributionDf,Seq(Year),InnerJoin)


  }

  def getFilePath(usFile:String,worldFile:String)  = {
    Try(getClass.getResource(s"/input/problem3Input/$usFile").getPath,
        getClass.getResource(s"/input/problem3Input/$worldFile").getPath)
  }


  def readInputByType(inputType:String) = {
    inputType match  {
      case `BarleyType` => val schema =  ScalaReflection.schemaFor[Barley].dataType.asInstanceOf[StructType]
        val dfs =  readHandler(schema,USBarley,WorldBarley)
        (cleanData(dfs._1,Harvest),cleanData(dfs._2,Harvest))

      case `BeefType` =>   val schema = ScalaReflection.schemaFor[Beef].dataType.asInstanceOf[StructType]
                                            val dfs = readHandler(schema,USBeef,WorldBeef)
                                                     (cleanData(dfs._1,Slaughter),cleanData(dfs._2,Slaughter))

      case `CornType` =>   val schema = ScalaReflection.schemaFor[Barley].dataType.asInstanceOf[StructType]
                                            val dfs =  readHandler(schema,USCorn,WorldCorn)
                                            (cleanData(dfs._1,Harvest),cleanData(dfs._2,Harvest))

      case `CottonType` =>  val schema = ScalaReflection.schemaFor[Barley].dataType.asInstanceOf[StructType]
                                         val dfs = readHandler(schema,USCotton,WorldCotton)
                                         (cleanData(dfs._1,Harvest),cleanData(dfs._2,Harvest))

      case  `PorkType` =>  val schema = ScalaReflection.schemaFor[Pork].dataType.asInstanceOf[StructType]
                                        val dfs =  readHandler(schema,USPork,WorldPork)
                                        (cleanData(dfs._1,Slaughter),cleanData(dfs._2,Slaughter))

      case  `PoultryType` => val schema = ScalaReflection.schemaFor[Poultry].dataType.asInstanceOf[StructType]
                                 val dfs =      readHandler(schema,USPoultry,WorldPoultry)
        (cleanData(dfs._1,Production),cleanData(dfs._2,Production))


      case `RiceType` =>   val schema = ScalaReflection.schemaFor[Rice].dataType.asInstanceOf[StructType]
        val dfs =  readHandler(schema,USRice,WorldRice)
        (cleanData(dfs._1,Harvest),cleanData(dfs._2,Harvest))


      case `SorghumType` =>  val schema = ScalaReflection.schemaFor[Barley].dataType.asInstanceOf[StructType]
        val dfs =       readHandler(schema,USSorghum,WorldSorghum)
        (cleanData(dfs._1,Harvest),cleanData(dfs._2,Harvest))

      case `SoybeanType` =>  val schema = ScalaReflection.schemaFor[Soybean].dataType.asInstanceOf[StructType]
        val dfs =             readHandler(schema,USSoybean,WorldSoybean)
        (cleanData(dfs._1,Harvest),cleanData(dfs._2,Harvest))

      case `SoybeanMealType` =>  val schema = ScalaReflection.schemaFor[SoybeanMeal].dataType.asInstanceOf[StructType]
        val dfs =  readHandler(schema,USSoybeanMeal,WorldSoybeanMeal)
        (cleanData(dfs._1,Harvest),cleanData(dfs._2,Harvest))

      case `SoybeanOilType` =>  val schema = ScalaReflection.schemaFor[SoybeanMeal].dataType.asInstanceOf[StructType]
        val dfs =   readHandler(schema,USSoybeanOil,WorldSoybeanOil)
        (cleanData(dfs._1,Harvest),cleanData(dfs._2,Harvest))

      case `WheatType` =>  val schema = ScalaReflection.schemaFor[Barley].dataType.asInstanceOf[StructType]
        val dfs =  readHandler(schema,USWheat,WorldWheat)
        (cleanData(dfs._1,Harvest),cleanData(dfs._2,Harvest))
    }
  }

  def readHandler (schema:StructType,
                       usFilepath:String,
                       worldFilePath:String) ={
    getFilePath(usFilepath,worldFilePath) match {
      case Success(paths) =>
        val usDf = readInputAsDf(paths._1,schema)
        val worldDf = readInputAsDf(paths._2,schema)
        (Some(usDf),Some(worldDf))
      case Failure(exception) => println("Exception while fetchingFile paths")
        exception.printStackTrace()
        (None,None)}
  }

  def cleanData(inpDf:Option[DataFrame],colName:String) ={

    inpDf match {
      case Some(df) => Some(df.withColumn(colName,when(col(colName) === ("--").trim,0)
                                             .otherwise(col(colName)))
        .withColumn(Year,when(col(Year).contains("/"),col(Year).substr(0,5))
          .otherwise(col(Year)))
        .withColumn(Year,trim(col(Year))))
      case None =>  None
    }
  }


 def readInputAsDf(path:String,schema:StructType) ={
   SparkUtility.spark
     .read
     .option("header","true")
     .schema(schema)
     .csv(path)
 }

  def findUsContribution(inputDfs:(Option[DataFrame],Option[DataFrame]),
                         metric:String) = {
    metric match {
      case `UsBarleyContribution` | `UsCornContribution` |
      `UsCottonContribution`|`UsRiceContribution` |
      `UsSorghumContribution` | `UsSoybeanContribution`
      |`UsWheatContribution` =>
        joinDataFrameByCol(inputDfs,Harvest,metric)

      case `UsBeefContribution` | `UsPorkContribution` => joinDataFrameByCol(inputDfs,Slaughter,metric)

      case `UsPoultryContribution` |`UsSoybeanMealContribution` | `UsSoybeanOilContribution`
      =>   joinDataFrameByCol(inputDfs,Production,metric)

    }

  }

  def joinDataFrameByCol(inputDfs:(Option[DataFrame],Option[DataFrame]),
                         computeCol:String,
                         metric:String) ={
    val usDf = inputDfs._1.get
    val worldDf = inputDfs._2.get
    val wordHarvest = s"world${metric.substring(2,metric.length-1)}"

    val joinedDf = usDf.join(worldDf,usDf(Year) === worldDf(Year),InnerJoin)
                       .withColumn(metric,(usDf(computeCol)/worldDf(computeCol))*100)

    joinedDf.select(usDf(Year),joinedDf(metric),worldDf(computeCol).alias(wordHarvest))

    }

}
