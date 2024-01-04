import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._

object Netflix {
  case class Rating(movieID: Int, userID: Int, rating: Int, date: String)
  case class Movie(movieID: Int, year: Int, title: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Netflix")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ratings: RDD[Rating] = sc.textFile(args(0)).map { line =>
      val fields = line.split(",")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3))
    }

    val movies: RDD[Movie] = sc.textFile(args(1)).map { line => {
      val a = line.split(",", 3);
      val year = if (a(1) != "NULL") a(1).toInt else 0
      Movie(a(0).toInt, year, a(2))
    }
    }

    ratings.toDF.createOrReplaceTempView("R")
    movies.toDF.createOrReplaceTempView("M")

    val result = spark.sql(
      """
     SELECT
       COALESCE(M.year, 0) AS year,
       CONCAT_WS(' ', M.title, CAST(M.year AS STRING)) AS title,
       AVG(COALESCE(R.rating, 0)) AS avg
     FROM M
     LEFT JOIN R ON M.movieID = R.movieID
     GROUP BY year, title
     HAVING avg > 0
     ORDER BY avg DESC
     LIMIT 100
     """
    )

    result.collect.foreach { case Row(year: Int, title: String, avg: Double) =>
      println(f"$avg%1.2f\t$year\t$title")
    }
  }
}

