import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import java.io._

object vatsal_sharma_task2 {
  def main(args: Array[String])= {

    val spark = SparkSession.builder().master("local[*]").appName("task1").getOrCreate()
    val rdd_review = spark.read.json(args(0)).rdd.map(x => (x.getAs[String]("business_id"),x.getAs[Double]("stars")))
    val rdd_business = spark.read.json(args(1)).rdd.map(x => (x.getAs[String]("business_id"),x.getAs[String]("state")))
    val rdd_join_review_business = rdd_business.join(rdd_review)
    val rdd_state_stars = rdd_join_review_business.map(x => x._2).groupByKey().mapValues(x => x.sum / x.size).sortBy(x => (-x._2,x._1))
    val list_rdd = rdd_state_stars.collect().toList
    val file = args(2)
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    writer.write("state,stars" + "\n")
    for (x <- list_rdd) {
      writer.write(x._1.toString() + ',' + x._2.toString() + "\n")  // however you want to format it
    }
    writer.close()

    var start = System.nanoTime
    val list_method_1 = rdd_state_stars.collect()

    for (x <- 0 to 4){
      println(list_method_1(x))
    }
    var end = System.nanoTime
    val time1 = end - start

    start = System.nanoTime
    val list_method_2 = rdd_state_stars.take(5)

    list_method_2.foreach(println)

    end = System.nanoTime

    val time2 = end - start

    val output = scala.collection.immutable.ListMap("m1" -> time1, "m2" -> time2, "explanation" -> "The time here is in nanoseconds. Method 1 takes more time as first we collect the entire data and then print first 5 states whereas in second method we take first 5 states from sorted rdd and print it.")

    var json_object = scala.util.parsing.json.JSONObject(output)

    var file1 = new FileWriter(args(3))
    file1.write(json_object.toString())
    file1.flush()
  }
}

