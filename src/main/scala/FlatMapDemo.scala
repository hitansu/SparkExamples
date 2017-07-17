import org.apache.spark.sql.SparkSession

/**
  * Created by hitansuj on 7/6/17.
  */
 object FlatMapDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
                            .master("local")
                            .appName("Word Count Scala")
                            .getOrCreate()
    val lines= spark.sparkContext.parallelize(Seq("Spark Intellij Idea Scala test one",
                                                  "Spark Intellij Idea Scala test two",
                                                  "Spark Intellij Idea Scala test three"))

    val words= lines.flatMap(s=>s.split(" "))
    // get total word count
    println(words.count())

    //print each word
    words.foreach(println)

    //print distinct word
    val distinct= words.distinct()
    distinct.foreach(println)

    //print duplicate word
    val dupwords= words.subtract(distinct).distinct()
    dupwords.foreach(println)

  }
}
