import org.apache.spark.sql.SparkSession
import scala.util.Random
import java.io.PrintWriter

object DataGenerator {
  def randomInt(min: Int, max: Int): Int = {
    min + Random.nextInt((max - min) + 1)
  }

  def randomDouble(min: Double, max: Double): Double = {
    min + (max - min) * Random.nextDouble()
  }

  val firstNames = Seq(
    "John", "Emily", "Michael", "Sarah", "David", "Emma", "Daniel", "Olivia", "James", "Sophia",
    "William", "Isabella", "Alexander", "Mia", "Ethan", "Charlotte", "Henry", "Amelia", "Lucas", "Ava",
    "Benjamin", "Ella", "Sebastian", "Lily", "Jack", "Grace", "Matthew", "Hannah", "Jacob", "Chloe", "Emma", "Taeha",
    "Elene", "Rowan"
  )

  val lastNames = Seq(
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
    "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Lin", "Song",
    "Kajaia", "Faulkner"
  )

  val streets = Seq(
    "Main St", "Oak St", "Pine Ave", "Maple Rd", "Cedar Blvd", "Elm St", "Walnut St", "Park Ave", "2nd St", "Sunset Blvd",
    "Dover St", "Institute St"
  )

  val transDescs = Seq(
    "Electronics Purchase", "Grocery Shopping", "Online Subscription", "Clothing Store",
    "Restaurant Dinner", "Gas Station", "Bookstore Purchase", "Pharmacy Order",
    "Movie Tickets", "Furniture Store", "Gym Membership", "Streaming Service Payment",
    "Coffee Shop", "Pet Store Purchase", "Hotel Booking", "Flight Ticket", "Taxi Ride", "Concert Ticket"
  )

  def randomName(): String = {
    s"${firstNames(Random.nextInt(firstNames.length))} ${lastNames(Random.nextInt(lastNames.length))}"
  }

  def randomAddress(): String = {
    s"${randomInt(100, 9999)} ${streets(Random.nextInt(streets.length))}"
  }

  def randomTransDesc(): String = {
    transDescs(Random.nextInt(transDescs.length))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataGenerator")
      .master("local[*]")
      .getOrCreate()

    val customersFile = "Customers.csv"
    val purchasesFile = "Purchases.csv"

    val numCustomers = 50000
    val numPurchases = 5000000

    println("Generating Customers...")
    val customersWriter = new PrintWriter(customersFile)

    // ✅ Add header to Customers.csv
    customersWriter.println("CustID,Name,Age,Address,Salary")

    for (custID <- 1 to numCustomers) {
      val name = randomName()
      val age = randomInt(18, 100)
      val address = randomAddress()
      val salary = f"${randomDouble(1000.0, 10000.0)}%.2f"
      customersWriter.println(s"$custID,$name,$age,$address,$salary")
    }
    customersWriter.close()
    println(s"$numCustomers customers written to $customersFile.")

    println("Generating Purchases...")
    val purchasesWriter = new PrintWriter(purchasesFile)

    // ✅ Add header to Purchases.csv
    purchasesWriter.println("TransID,CustID,TransTotal,TransNumItems,TransDesc")

    for (transID <- 1 to numPurchases) {
      val custID = randomInt(1, numCustomers)
      val transTotal = f"${randomDouble(10.0, 2000.0)}%.2f"
      val transNumItems = randomInt(1, 15)
      val transDesc = randomTransDesc()
      purchasesWriter.println(s"$transID,$custID,$transTotal,$transNumItems,$transDesc")
    }
    purchasesWriter.close()
    println(s"$numPurchases purchases written to $purchasesFile.")

    spark.stop()
  }
}
