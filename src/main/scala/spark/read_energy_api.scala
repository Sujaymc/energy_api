package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import requests._

object read_energy_api {

  def main(args: Array[String]): Unit = {
    // Start Spark session
    val spark = SparkSession.builder()
      .appName("Alpha Vantage Intraday Reader")
      .master("local[*]")
      .getOrCreate()

    while (true) {
      import spark.implicits._

      // API details
      val apiUrl = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=T9VJ3JP8JPENROGG"
      val response = get(apiUrl)
      val jsonResponse = response.text()

      // Define schema for the time series data
      val timeSeriesSchema = StructType(Seq(
        StructField("timestamp", StringType, true),
        StructField("open", StringType, true),
        StructField("high", StringType, true),
        StructField("low", StringType, true),
        StructField("close", StringType, true),
        StructField("volume", StringType, true)
      ))

      // Parse JSON response
      val rawDF = spark.read.json(Seq(jsonResponse).toDS())

      // Extract the "Time Series (5min)" data
      val timeSeriesDF = rawDF
        .select(explode($"`Time Series (5min)`").alias("timestamp", "values"))
        .select(
          $"timestamp",
          $"values.`1. open`".alias("open"),
          $"values.`2. high`".alias("high"),
          $"values.`3. low`".alias("low"),
          $"values.`4. close`".alias("close"),
          $"values.`5. volume`".alias("volume")
        )

      // Show a few rows for debugging
      timeSeriesDF.show(5, truncate = false)

      // Kafka server and topic name assignment
      val kafkaServer: String = "ip-172-31-8-235.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "alpha_vantage_topic" // Your Kafka topic name

      // Write data to Kafka
      timeSeriesDF.selectExpr("CAST(timestamp AS STRING) AS key", "to_json(struct(*)) AS value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaServer)
        .option("topic", topicSampleName)
        .save()

      println("Message is loaded to Kafka topic")
      Thread.sleep(60000) // Wait for 1 minute before making the next call
    }
  }
}
