import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import java.io.FileWriter

object vatsal_sharma_task1 {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().master("local[*]").appName("task1").getOrCreate()
    val rdd_json = spark.read.json(args(0)).rdd
    val useful = rdd_json.filter(x => x.getAs[Long]("useful") > 0).count()
    val stars = rdd_json.filter(x => x.getAs[Double]("stars") == 5).count()
    val longest_review = rdd_json.map(x => x.getAs[String]("text").length()).max()
    val rdd_cust = rdd_json.map(x => (x.getAs[String]("user_id"), 1)).reduceByKey((a, b) => a + b).sortBy(x => (-x._2, x._1))
    val total_cust = rdd_cust.count()
    val rdd_business = rdd_json.map(x => (x.getAs[String]("business_id"), 1)).reduceByKey((a, b) => a + b).sortBy(x => (-x._2, x._1))
    val total_business = rdd_business.count()
    val top_20_cust = rdd_cust.take(20).toList
    val top_20_business = rdd_business.take(20).toList

    var file1 = new FileWriter(args(1))
    file1.write("{")
    file1.write("\n")
    file1.write("  \"n_review_useful\"" + ": " + useful + ",")
    file1.write("\n")
    file1.write("  \"n_review_5_star\"" + ": " + stars+ ",")
    file1.write("\n")
    file1.write("  \"n_characters\"" + ": " + longest_review+ ",")
    file1.write("\n")
    file1.write("  \"n_user\"" + ": " + total_cust+ ",")
    file1.write("\n")
    file1.write("  \"top20_user\"" + ": " + "[")
    for (x <- 0 to 18){
      file1.write("[\"" + top_20_cust(x)._1 + "\"" + "," + top_20_cust(x)._2 + "],")
    }
    file1.write("[\"" + top_20_cust(19)._1 + "\"" + "," + top_20_cust(19)._2 + "]],")
    file1.write("\n")
    file1.write("  \"n_business\"" + ": " + total_business+ ",")
    file1.write("\n")
    file1.write("  \"top20_business\"" + ": " + "[")
    for (x <- 0 to 18){
      file1.write("[\"" + top_20_business(x)._1 + "\"" + "," + top_20_business(x)._2 + "],")
    }
    file1.write("[\"" + top_20_business(19)._1 + "\"" + "," + top_20_business(19)._2 + "]]")
    file1.write("\n")
    file1.write("}")
    file1.flush()
  }

}