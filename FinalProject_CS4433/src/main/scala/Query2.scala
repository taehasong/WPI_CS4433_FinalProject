import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object Query2 {
  def median(list: Seq[Double]): Double = {
    val sortedList = list.sorted
    val size = sortedList.size
    if (size % 2 == 0) {
      (sortedList(size / 2 - 1) + sortedList(size / 2)) / 2.0
    } else {
      sortedList(size / 2)
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Query2")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val t1Schema = StructType(Array(
      StructField("TransID", IntegerType, nullable = false),
      StructField("CustID", IntegerType, nullable = false),
      StructField("TransTotal", DoubleType, nullable = false),
      StructField("TransNumItems", IntegerType, nullable = false),
      StructField("TransDesc", StringType, nullable = false)
    ))

    val t1DF = spark.read
      .option("header", "true")
      .schema(t1Schema)
      .csv("T1.csv")

    t1DF.show(5)

    // Group by TransNumItems and collect TransTotal values
    val groupedDF = t1DF.groupBy("TransNumItems")
      .agg(
        F.collect_list("TransTotal").alias("TransTotalList"),
        F.min("TransTotal").alias("MinTransTotal"),
        F.max("TransTotal").alias("MaxTransTotal")
      )

    // Register UDF for median
    val medianUDF = F.udf(median _)

    // Apply median calculation
    val resultDF = groupedDF.withColumn("MedianTransTotal", medianUDF($"TransTotalList"))
      .drop("TransTotalList")

    println("Query 2 Results (Grouped by TransNumItems):")
    resultDF.orderBy("TransNumItems").show(100, truncate = false)

    spark.stop()
  }
}
