import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator

object PrintR2Values {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("R2ValuePrinter")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load data (similar to your original code)
    val purchasesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Purchases.csv")

    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Customers.csv")

    var dataset = purchasesDF.join(customersDF, "CustID")
      .select($"CustID", $"TransID", $"Age", $"Salary", $"TransNumItems", $"TransTotal")

    // Data cleaning
    dataset = dataset.filter($"TransTotal" < 10000 && $"Salary" > 0)
      .withColumn("LogSalary", log($"Salary"))

    // Feature assembly
    val featureCols = Array("Age", "LogSalary", "TransNumItems")
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val finalData = assembler.transform(dataset)
      .select($"features", $"TransTotal".alias("label"))

    // Split data
    val Array(trainSet, testSet) = finalData.randomSplit(Array(0.8, 0.2), seed = 1234)

    // Regression Evaluator
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")

    // Linear Regression
    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
    val lrModel = lr.fit(trainSet)
    val lrPredictions = lrModel.transform(testSet)
    val lrR2 = evaluator.setMetricName("r2").evaluate(lrPredictions)

    // Random Forest
    val rf = new RandomForestRegressor()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setNumTrees(100)
    val rfModel = rf.fit(trainSet)
    val rfPredictions = rfModel.transform(testSet)
    val rfR2 = evaluator.setMetricName("r2").evaluate(rfPredictions)

    // Detailed R-squared and additional metrics printing
    println("Linear Regression Metrics:")
    println(s"R-squared (R²): $lrR2")
    println(s"Root Mean Squared Error (RMSE): ${evaluator.setMetricName("rmse").evaluate(lrPredictions)}")
    println(s"Mean Absolute Error (MAE): ${evaluator.setMetricName("mae").evaluate(lrPredictions)}")

    println("\nRandom Forest Metrics:")
    println(s"R-squared (R²): $rfR2")
    println(s"Root Mean Squared Error (RMSE): ${evaluator.setMetricName("rmse").evaluate(rfPredictions)}")
    println(s"Mean Absolute Error (MAE): ${evaluator.setMetricName("mae").evaluate(rfPredictions)}")

    // Print additional diagnostic information
    println("\nLinear Regression Coefficients:")
    println(lrModel.coefficients)
    println(s"Intercept: ${lrModel.intercept}")

    println("\nRandom Forest Feature Importances:")
    println(rfModel.featureImportances)

    spark.stop()
  }
}