import org.apache.spark.sql.SparkSession

object CheckTransNumItems {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CheckTransNumItems")
      .master("local[*]")
      .getOrCreate()

    // Load T1.csv
    val t1DF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("T1.csv")

    // Show distinct TransNumItems values
    println("✅ Unique TransNumItems values:")
    t1DF.select("TransNumItems")
      .distinct()
      .orderBy("TransNumItems")
      .show(100, false)

    // Count how many records per TransNumItems
    println("✅ Count of records per TransNumItems:")
    t1DF.groupBy("TransNumItems")
      .count()
      .orderBy("TransNumItems")
      .show(100, false)

    spark.stop()
  }
}
