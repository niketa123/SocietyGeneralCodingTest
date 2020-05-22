package com.codingtest.probelm4.GraphSoln

object TestGraph  extends App {

  val adjList =Map("Soiety_Generale" -> List("Credit Agricole","UBS"),
    "Credit Agricole" -> List("HSBC","BNP","Boursorama"),
    "UBS" -> List("RBS"),
    "HSBC" -> List("Santander"),
    "BNP" -> List("Boursorama"),
    "RBS" -> List("Deutsche"))

  val g = new DirectedGraph[String](adjList)

  println(g)


}
