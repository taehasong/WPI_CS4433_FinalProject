import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestSpark")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(("Hello"), ("World"))
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("Word")

    df.show()

    spark.stop()
  }
}
