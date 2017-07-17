import org.apache.spark.sql.SparkSession

/**
  * Created by hitansuj on 7/6/17.
  */
object UnionDemo {

  def main(args: Array[String]): Unit = {
     val spark= SparkSession.builder()
                    .master("local")
                    .appName("union demo")
                    .getOrCreate();

    val rdd_1= spark.sparkContext.parallelize(Seq(200, 204, 208, 203, 205, 207))
    val rdd_2= spark.sparkContext.parallelize(Seq(200, 104, 208, 108, 107, 207))

    val union_rdd= rdd_1.union(rdd_2).collect()
    val common_rdd= rdd_1.intersection(rdd_2).collect()
    val diff_frm_rdd2= rdd_1.subtract(rdd_2).collect()

    // you can save the
   // rdd_1.saveAsTextFile("")
    union_rdd.foreach(println)
    println("******")
    common_rdd.foreach(x=>println(x)) // we can write the println like this also
    println("******")
    diff_frm_rdd2.foreach(println)


  }
}
