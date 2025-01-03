package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
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

      // Define schema explicitly
      val schema = StructType(Seq(
        StructField("count", IntegerType, true),
        StructField("next", StringType, true),
        StructField("previous", StringType, true),
        StructField("results", ArrayType(
          StructType(Seq(
            StructField("code", StringType, true),
            StructField("direction", StringType, true),
            StructField("full_name", StringType, true),
            StructField("display_name", StringType, true),
            StructField("description", StringType, true),
            StructField("is_variable", BooleanType, true),
            StructField("is_green", BooleanType, true),
            StructField("is_tracker", BooleanType, true),
            StructField("is_prepay", BooleanType, true),
            StructField("is_business", BooleanType, true),
            StructField("is_restricted", BooleanType, true),
            StructField("term", StringType, true),
            StructField("available_from", StringType, true),
            StructField("available_to", StringType, true),
            StructField("links", ArrayType(
              StructType(Seq(
                StructField("href", StringType, true),
                StructField("method", StringType, true),
                StructField("rel", StringType, true)
              ))
            ), true),
            StructField("brand", StringType, true)
          ))
        ), true)
      ))

      // Parse JSON with schema
      val dfFromText = spark.read.schema(schema).json(Seq(total).toDS)

      // Flatten the results array
      val resultsDF = dfFromText
        .selectExpr("inline(results) as result")
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
//spark-submit --master local --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7","com.lihaoyi:requests_2.11:0.7.1" --class spark.sop_read_api target/EnergyAPIReader-1.0-SNAPSHOT.jar
