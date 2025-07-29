import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Query4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Query4")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //  Load T1.csv (transactions)
    val transactionsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("T1.csv")

    //  Load Customers.csv with correct header
    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Customers.csv")

    //  Verify schemas (optional but helpful)
    println("Transactions schema:")
    transactionsDF.printSchema()
    println("Customers schema:")
    customersDF.printSchema()

    // Calculate total expenses per customer from T1
    val totalExpensesDF = transactionsDF
      .groupBy("CustID")
      .agg(
        sum("TransTotal").alias("TotalExpenses")
      )
    println("✅ Total expenses per customer:")
    totalExpensesDF.orderBy(desc("TotalExpenses")).show(20, truncate = false)


    // Join with Customers on CustID (explicit join)
    val joinedDF = totalExpensesDF
      .join(customersDF, totalExpensesDF("CustID") === customersDF("CustID"))
      .select(
        customersDF("CustID"),
        $"TotalExpenses",
        $"Salary",
        $"Address"
      )

    println("✅ Joined data (TotalExpenses + Salary):")
    joinedDF.orderBy(desc("TotalExpenses")).show(20, truncate = false)

    //  Filter customers who can't cover their expenses
    val resultDF = joinedDF.filter($"Salary" < $"TotalExpenses")

    //  Show result
    println("❌ Customers who can't cover their expenses:")
    resultDF.show()

    spark.stop()
  }
}
