import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Graph {
  val start_id = 14701391
  val max_int = Int.MaxValue
  val iterations = 5

  def main ( args: Array[ String ] ) {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)

    val graph: RDD[(Int,Int)]
    = sc.textFile(args(0))
      .map(line => {
        val a = line.split(",")
        (a(1).toInt, a(0).toInt)

      })               // create a graph edge (i,j), where i follows j

    var R: RDD[(Int,Int)]             // initial shortest distances
    = graph.groupByKey()
      .map(edge => {
        if (edge._1 == start_id || edge._1 == 1) (edge._1, 0)
        else (edge._1, max_int)
      })            // starting point has distance 0, while the others max_int

    for (i <- 0 until iterations) {
      R = R.join(graph)
        .flatMap(
          {
            case (i, (d, j)) =>
              if (d < max_int) {
                List((j, d + 1), (i, d))
              } else {
                List((i, d))
              }
          }

        )            // calculate distance alternatives
        .reduceByKey( (a, b) => Math.min(a, b) )        // for each node, find the shortest distance
    }

    R.filter( edge => edge._2 < max_int )                    // keep only the vertices that can be reached
      .map(edge => (edge._2, 1) )                       // prepare for the reduceByKey
      .reduceByKey(_ + _)               // for each different distance, count the number of nodes that have this distance
      .sortByKey()
      .collect()
      .foreach(println)
  }
}
