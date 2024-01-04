import org.apache.spark.graphx.{Graph,Edge,EdgeTriplet,VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object PageRank {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    val a = 0.85

    // read the input graph
    val es: RDD[(Long,Long)]
    = sc.textFile(args(0))
      .map {line => val lineSplit = line.split(",")
        (lineSplit(1).toLong, lineSplit(0).toLong)
      }

    // Graph edges have attribute values 0.0
    val edges: RDD[Edge[Double]] = es.map { case (src, dst) => Edge(src, dst, 0.0) }

    // graph vertices with their degrees (# of outgoing neighbors)
    val degrees: RDD[(Long,Int)] = edges.map(e => (e.srcId, 1)).reduceByKey(_ + _)

    // degrees.count must be 9191 for small data
    val TotalVertex = degrees.count
    // initial pagerank
    val init = 1.0/TotalVertex

    // graph vertices with attribute values (degree,rank), where degree is the # of
    // outgoing neighbors and rank is the vertex pagerank (initially = init)
    val vertices: RDD[(Long,(Int,Double))] = degrees.map { case (id, degree) => (id, (degree, init)) }

    // the GraphX graph
    val graph: Graph[(Int,Double),Double] = Graph(vertices,edges,(0,init))

    def newValue(id: VertexId, currentValue: (Int, Double), newrank: Double): (Int, Double) = {
      val (degree, _) = currentValue
      val pr = (1.0 - a) / TotalVertex + a * newrank
      (degree, pr)
    }

    def sendMessage(triplet: EdgeTriplet[(Int, Double), Double]): Iterator[(VertexId, Double)] = {
      Iterator((triplet.dstId, triplet.srcAttr._2 / triplet.srcAttr._1))
    }

    def mergeValues(x: Double, y: Double): Double = x + y

    // calculate PageRank using pregel
    val pagerank = graph.pregel(init, 10)(
      newValue,
      sendMessage,
      mergeValues
    )


    // Print the top 30 results
    pagerank.vertices.sortBy(_._2._2,false,1).take(30)
      .foreach{ case (id,(_,p)) => println("%12d\t%.6f".format(id,p)) }

  }
}
