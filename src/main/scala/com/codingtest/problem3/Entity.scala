package com.codingtest.problem3

case class Barley(year:String,
                  harvest:Long,
                  Yield:Float,
                  Production:Long,
                  Imports:Long,
                  Exports:Long,
                  TotalConsumption:Long,
                  industrialUse:Long,
                  FeedUse:Long,
                  EndingStocks:Long)

case class Beef(year:String,
                Slaughter:Option[String]=None,
                Yield:Option[String]=None,
                Production:Long,
                Imports:Long,
                Exports:Long,
                TotalConsumption:Long,
                EndingStocks:Long)

case class Cotton(year:String,
                  harvest:Long,
                  Yield:Float,
                  Production:Long,
                  Imports:Long,
                  Exports:Long,
                  TotalConsumption:Long,
                  UnAccountingLoss:Long,
                  EndingStocks:Long)

case class Pork(year:String,
                    Slaughter:Long,
                    Imports:Long,
                    Exports:Long,
                    TotalConsumption:Long,
                    EndingStocks:Long)

case class Poultry(year:String,
                Production:Long,
                Imports:Long,
                Exports:Long,
                TotalConsumption:Long,
                EndingStocks:Long)

case class Rice(year:String,
                  harvest:Long,
                  Yield:Float,
                  Production:Long,
                  Imports:Long,
                  Exports:Long,
                  TotalConsumption:Long,
                  EndingStocks:Long)

case class Soybean(year:String,
                   harvest:String,
                   Yield:String,
                   Production:Long,
                   Imports:Long,
                   Exports:Long,
                   TotalConsumption:Long,
                   FoodUse:Long,
                   FeedWaste:Long,
                   Crush:Long,
                   EndingStocks:Long)

case class SoybeanMeal(year:String,
                       ExtractionRate:Double,
                   harvest:String,
                   Yield:String,
                   Production:Long,
                   Imports:Long,
                   Exports:Long,
                   TotalConsumption:Long,
                   FoodUse:Long,
                   FeedUse:Long,
                   Crush:Long,
                   EndingStocks:Long)