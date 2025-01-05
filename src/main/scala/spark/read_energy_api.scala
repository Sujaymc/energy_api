package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import requests._

object ReadEnergyAPI {

  def main(args: Array[String]): Unit = {
    // Start Spark session
    val spark = SparkSession.builder()
      .appName("Alt Fuel Stations API Reader")
      .master("local[*]")
      .getOrCreate()

    while (true) {
      import spark.implicits._

      // API details
      val apiUrl = "https://developer.nrel.gov/api/alt-fuel-stations/v1.json?QadijL96MjkrLZoz3JJmisWblq4fdFv0fbTC7cA57"
      val response = get(apiUrl)
      val total = response.text()

      // Define schema explicitly for the full structure
      val schema = StructType(Seq(
        StructField("station_locator_url", StringType, true),
        StructField("total_results", IntegerType, true),
        StructField("station_counts", StructType(Seq(
          StructField("total", IntegerType, true),
          StructField("fuels", StructType(Seq(
            StructField("E85", StructType(Seq(
              StructField("total", IntegerType, true)
            ))),
            StructField("ELEC", StructType(Seq(
              StructField("total", IntegerType, true),
              StructField("stations", StructType(Seq(
                StructField("total", IntegerType, true)
              )))
            )))
          )))
        ))),
        StructField("fuel_stations", ArrayType(
          StructType(Seq(
            StructField("access_code", StringType, true),
            StructField("access_days_time", StringType, true),
            StructField("fuel_type_code", StringType, true),
            StructField("station_name", StringType, true),
            StructField("latitude", DoubleType, true),
            StructField("longitude", DoubleType, true),
            StructField("city", StringType, true),
            StructField("state", StringType, true),
            StructField("street_address", StringType, true),
            StructField("ev_connector_types", ArrayType(StringType, true), true),
            StructField("ev_network", StringType, true),
            StructField("ev_network_web", StringType, true),
            StructField("ev_pricing", StringType, true),
            StructField("ev_renewable_source", StringType, true),
            StructField("ev_workplace_charging", BooleanType, true)
            // Add other fields if needed
          ))
        ), true)
      ))

      // Parse JSON with schema
      val dfFromText = spark.read.schema(schema).json(Seq(total).toDS)

      // Flatten the fuel_stations array
      val fuelStationsDF = dfFromText
        .select(explode($"fuel_stations").alias("station"))  // Flatten the array
        .select(
          $"station.station_name".alias("station_name"),
          $"station.fuel_type_code".alias("fuel_type_code"),
          $"station.latitude".alias("latitude"),
          $"station.longitude".alias("longitude"),
          $"station.city".alias("city"),
          $"station.state".alias("state"),
          $"station.street_address".alias("street_address"),
          $"station.ev_connector_types".alias("ev_connector_types"),
          $"station.ev_network".alias("ev_network"),
          $"station.ev_network_web".alias("ev_network_web"),
          $"station.ev_pricing".alias("ev_pricing"),
          $"station.ev_renewable_source".alias("ev_renewable_source"),
          $"station.ev_workplace_charging".alias("ev_workplace_charging")
        )

      // Show a few rows for debugging
      fuelStationsDF.show(5, truncate = false)

      // Kafka server and topic name assignment
      val kafkaServer: String = "ip-172-31-8-235.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "sujay_topic1" // Your Kafka topic name

      // Write data to Kafka
      fuelStationsDF.selectExpr("CAST(station_name AS STRING) AS key", "to_json(struct(*)) AS value")
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
//spark-submit --master local --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7","com.lihaoyi:requests_2.11:0.7.1" --class spark.ReadEnergyAPI target/EnergyAPIReader-1.0-SNAPSHOT.jar
