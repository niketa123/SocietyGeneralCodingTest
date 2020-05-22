package com.codingtest.probelm4.GraphSoln

class DirectedGraph[V](adjList:Map[V,List[V]]) extends Graph[V] {
  override def getVertices: List[V] = adjList.keys.toList

  override def getEdges: List[(V, V)] = adjList.flatMap{
    case (v,neighbours) => neighbours.map(n => (v,n))
  }.toList

  override def addEdge(a:V,b:V): Graph[V] = {

    val aNeighbours = b +: neighbours(a)

    new DirectedGraph[V](adjList+(a -> aNeighbours))

  }

  override def neighbours(vertex: V): List[V] = adjList.getOrElse(vertex,Nil)
}
