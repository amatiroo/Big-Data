import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object Netflix {
  def main(args: Array[ String ]): Unit = {
    val conf = new SparkConf().setAppName("Netflix")
    val sc = new SparkContext(conf)

    val ratings: RDD[(Int,Int)]
      = sc.textFile(args(0)).map { line => { val a = line.split(","); (a(0).toInt,a(2).toInt)}
    }

    val groupData = ratings.groupBy(_._1)

    val avgRatings = groupData.map { case (movieId, ratings) =>
      val totalRating = ratings.map(x => x._2).sum
      val length = ratings.size
      val averageRating = totalRating.toDouble / length
      (movieId, averageRating)
    }

    val movies: RDD[(Int,Int,String)]
      = sc.textFile(args(1)).map {
      line => { val a = line.split(",",3);
        val year = if (a(1) != "NULL") a(1).toInt else 0
        (a(0).toInt,year,a(2)) }
    }

    val avg_titles: RDD[(Double,String)]
      = {
        avgRatings.map(e => (e._1, e)).join(movies.map(d => (d._1,d )))
        .map { case (k, (e, d)) => (e._2,d._2 + ": " + d._3) }
    }

    avg_titles.sortBy(_._1, ascending = false)
      .map { case (avg,title) => "%.2f\t%s".format(avg,title) }
              .saveAsTextFile(args(2))

    sc.stop()

  }
}
