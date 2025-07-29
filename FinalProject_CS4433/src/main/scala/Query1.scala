import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._

object Query1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Query1")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val purchasesSchema = StructType(Array(
      StructField("TransID", IntegerType, nullable = false),
      StructField("CustID", IntegerType, nullable = false),
      StructField("TransTotal", DoubleType, nullable = false),
      StructField("TransNumItems", IntegerType, nullable = false),
      StructField("TransDesc", StringType, nullable = false)
    ))

    val purchasesDF = spark.read
      .option("header", "true") // Update this line
      .schema(purchasesSchema)
      .csv("Purchases.csv")

    purchasesDF.show(5)

    val t1DF = purchasesDF.filter($"TransTotal" <= 100)

    println("Filtered Purchases (<= $100):")
    t1DF.show(5)

    // Save as one CSV file
    t1DF
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("T1.csv")

    println("Filtered dataset T1 saved as a single CSV in T1.csv folder.")

    spark.stop()
  }
}
