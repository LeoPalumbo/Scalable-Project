import org.apache.spark.sql.SparkSession

object NeedlemanWunshAlignment extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Intersect")
    .getOrCreate()

  val x: String = "GCATGCT"
  val y: String = "GATACCA"
  val common_words = x.split("").toSet.intersect(y.split("").toSet)

  common_words.foreach(println)
}

object FindMinuminValue extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Intersect")
    .getOrCreate()

  //val x: Array[Array[Int]] = [[1; 2; 3; 4], [5; 6; 7; 8]]
  val initial_df = Seq(
    (4, 0, 0),
    (6, 1, 0),
    (1, 3, 2)
  )
  var matrix = Array(
    Array(1, 2, 3, 4),
    Array(5, 6, 7, 8),
    Array(9, 10, 11, 12),
    Array(13, 14, 13, 16))

  for (array <- matrix) {
    for (el <- array){
      //println(el.toString)

    }
  }

  for (array <- matrix) {
    println(array.min)
    println(array.indexOf(array.min))
  }
  //println(matrix.min(Array[Int]))

  val min = initial_df.min

  println(min)
  initial_df.foreach(println)
}