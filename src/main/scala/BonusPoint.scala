import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler, PolynomialExpansion}
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor, GBTRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.Pipeline

object BonusPoint {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ImprovedRegressionModel")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load data
    val purchasesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Purchases.csv")

    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Customers.csv")

    // Calculate the 99th percentile threshold for TransTotal
    val transotalStats = purchasesDF.agg(
      expr("percentile(TransTotal, 0.99)").alias("percentile_99")
    ).first()
    val percentile99 = transotalStats.getDouble(0)

    // Enhanced data preparation
    val dataset = purchasesDF.join(customersDF, "CustID")
      .select($"CustID", $"TransID", $"Age", $"Salary", $"TransNumItems", $"TransTotal")
      .filter($"TransTotal" > 0 && $"TransTotal" < percentile99)
      .filter($"Salary" > 0)
      // Add interaction and derived features
      .withColumn("AgeSalaryInteraction", $"Age" * $"Salary")
      .withColumn("LogSalary", log($"Salary" + 1))
      .withColumn("SalaryPerItem", $"Salary" / ($"TransNumItems" + 1))

    // Feature assembly
    val featureCols = Array("Age", "LogSalary", "TransNumItems", "AgeSalaryInteraction", "SalaryPerItem")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("rawFeatures")

    // Polynomial expansion to capture non-linear relationships
    val polyExpansion = new PolynomialExpansion()
      .setInputCol("rawFeatures")
      .setOutputCol("polyFeatures")
      .setDegree(2)

    // Standard scaling
    val scaler = new StandardScaler()
      .setInputCol("polyFeatures")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(true)

    // Regression models
    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("TransTotal")
      .setElasticNetParam(0.5)  // Mix of L1 and L2 regularization
      .setRegParam(0.1)

    val rf = new RandomForestRegressor()
      .setFeaturesCol("features")
      .setLabelCol("TransTotal")
      .setNumTrees(200)
      .setMaxDepth(10)

    val gbt = new GBTRegressor()
      .setFeaturesCol("features")
      .setLabelCol("TransTotal")
      .setMaxIter(100)

    // Split data
    val Array(trainSet, testSet) = dataset.randomSplit(Array(0.8, 0.2), seed = 42)

    // Evaluator
    val evaluator = new RegressionEvaluator()
      .setLabelCol("TransTotal")
      .setPredictionCol("prediction")

    // Perform evaluations for each model
    val models = Seq(
      ("Linear Regression", lr),
      ("Random Forest", rf),
      ("Gradient Boosted Trees", gbt)
    )

    models.foreach { case (modelName, model) =>
      val pipeline = new Pipeline().setStages(Array(assembler, polyExpansion, scaler, model))
      val pipelineModel = pipeline.fit(trainSet)
      val predictions = pipelineModel.transform(testSet)

      println(s"\n$modelName Metrics:")
      println(s"R-squared: ${evaluator.setMetricName("r2").evaluate(predictions)}")
      println(s"RMSE: ${evaluator.setMetricName("rmse").evaluate(predictions)}")
      println(s"MAE: ${evaluator.setMetricName("mae").evaluate(predictions)}")
    }

    spark.stop()
  }
}