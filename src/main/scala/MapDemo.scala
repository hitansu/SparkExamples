import org.apache.spark.sql.SparkSession

/**
  * Created by hitansuj on 7/6/17.
  *
  *
  */
object MapDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("Word Count Scala")
      .getOrCreate()

    var numbers= spark.sparkContext.parallelize(List(2, 3, 4, 6, 8, 9))
    var sqNumbers= numbers.map(num=>(num*num))
    var cubeNumbers= numbers.map(num=>(Math.pow(num, 3))).map(n=>n.toInt)
    sqNumbers.foreach(println)
    cubeNumbers.foreach(println)
  }
}
