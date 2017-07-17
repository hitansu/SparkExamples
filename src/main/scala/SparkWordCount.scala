import org.apache.spark.sql.SparkSession

/**
  * Created by hitansuj on 7/6/17.
  */
object SparkWordCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("Word Count Scala")
      .getOrCreate()
    val lines= spark.sparkContext.parallelize(Seq("Spark Intellij Idea Scala test one",
                                                  "Spark Intellij Idea Scala test two",
                                                  "Spark Intellij Idea Scala test three"))

    var count= lines.flatMap(line=>line.split(" "))
                    .map(word=>(word, 1))
                    .reduceByKey((x,y)=>x+y)
    count.foreach(println)
    count.saveAsTextFile("")


   /*finding top 4 words by count*/
  /*  val wordCounts = lines.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(item => item.swap)
      .sortByKey(false,1)
      .map(item => item.swap).takeOrdered(4);

    wordCounts.foreach(x=>println(x._1+"-->"+x._2))*/


  }

}
