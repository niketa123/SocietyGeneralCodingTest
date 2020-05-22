name := "SocietyGeneralTest_1"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.11" % "2.4.3",
                            "com.crealytics" %% "spark-excel" % "0.8.2",
  "org.apache.spark" %% "spark-core" % "2.3.2")