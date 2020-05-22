package com.codingtest.probelm4.GraphSoln

trait Graph[V] {

  def getVertices:List[V]

  def getEdges:List[(V,V)]

  // Returns new graph bcz creating immutable graph
  def addEdge(a:V,b:V):Graph[V]

  def neighbours(vertex:V):List[V]

}

object Graph{
  def apply[V](adjList:Map[V,List[V]]): Graph[V] = new DirectedGraph(adjList)
}
