package spark // Change to your directory name

import org.apache.spark.sql.SparkSession
import requests._

object read_energy_api {

  def main(args: Array[String]): Unit = {
    // Start Spark session
    val spark = SparkSession.builder()
      .appName("Energy Product API Reader")
      .master("local[*]")
      .getOrCreate()

    while (true) {
      import spark.implicits._

      // API details
      val apiUrl = "https://api.octopus.energy/v1/products/"
      val response = get(apiUrl)
      val total = response.text()
      val dfFromText = spark.read.json(Seq(total).toDS)

      // Extract the "results" array
      val resultsDF = dfFromText.select(explode($"results").alias("result"))
        .select(
          $"result.code".alias("code"),
          $"result.direction".alias("direction"),
          $"result.full_name".alias("full_name"),
          $"result.display_name".alias("display_name"),
          $"result.description".alias("description"),
          $"result.is_variable".alias("is_variable"),
          $"result.is_green".alias("is_green"),
          $"result.is_tracker".alias("is_tracker"),
          $"result.is_prepay".alias("is_prepay"),
          $"result.is_business".alias("is_business"),
          $"result.is_restricted".alias("is_restricted"),
          $"result.term".alias("term"),
          $"result.available_from".alias("available_from"),
          $"result.available_to".alias("available_to"),
          $"result.links".alias("links"),
          $"result.brand".alias("brand")
        )

      // Show a few rows for debugging
      resultsDF.show(5, truncate = false)

      // Kafka server and topic name assignment
      val kafkaServer: String = "ip-172-31-8-235.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "sujay_topic1" // Your Kafka topic name

      // Write data to Kafka
      resultsDF.selectExpr("CAST(code AS STRING) AS key", "to_json(struct(*)) AS value")
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaServer)
        .option("topic", topicSampleName)
        .save()

      println("Message is loaded to Kafka topic")
      Thread.sleep(10000) // Wait for 10 seconds before making the next call
    }
  }
}

//mvn package
//spark-submit --master local --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7","com.lihaoyi:requests_2.11:0.7.1" --class spark.read_energy_api target/energy_api-1.0-SNAPSHOT.jar
