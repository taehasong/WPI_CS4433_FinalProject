import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Query3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Query3")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load Customers.csv (with header)
    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Customers.csv")

    // Load T1.csv (with header)
    val purchasesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("T1.csv")

    // Filter Gen Z customers (Age between 18 and 21)
    val genZCustomersDF = customersDF.filter($"Age".between(18, 21))

    // Join Gen Z customers with their purchases
    val joinedDF = purchasesDF.join(genZCustomersDF, "CustID")

    // Group by CustID and Age, calculate total items and total amount
    val resultDF = joinedDF.groupBy("CustID", "Age")
      .agg(
        sum("TransNumItems").as("TotalItems"),
        sum("TransTotal").as("TotalAmount")
      )
      .orderBy("CustID")

    // Show result
    resultDF.show(20, truncate = false)

    // Save the result as T3.csv (single file)
    resultDF.coalesce(1)
      .write
      .option("header", "true")
      .csv("T3.csv")

    println("Query 3 completed. Results saved to T3.csv.")

    spark.stop()
  }
}
