import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by hitansuj on 7/6/17.
  */
object Sum {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("Word Count Scala")
      .getOrCreate()

    val numbers= spark.sparkContext.parallelize(List(1,2,3,4,5))
    val result_sum= numbers.reduce((a,b)=>a+b)
    println(result_sum)
  }

}
