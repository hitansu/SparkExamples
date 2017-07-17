import org.apache.spark.sql.SparkSession

/**
  * Created by hitansuj on 7/6/17.
  */
object Filtering {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
                            .master("local")
                            .appName("Word Count Scala")
                            .getOrCreate()

    val lines= spark.sparkContext.parallelize(Seq("Spark Intellij Idea Scala test one",
                                                  "error Idea Scala test two",
                                                  "Spark Intellij Idea error test three",
                                                  "abc def efg xyz found here",
                                                  "there is an error found",
                                                  "compilation error"))

    val errorLines= lines.filter(line=>line.contains("error"))
    errorLines.foreach(println)

    println("***** print first line ********")
    println(lines.first())
    println("***** print first 2 line *******")
    lines.take(2).foreach(println)
  }

}
