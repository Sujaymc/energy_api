package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import requests._

object read_energy_api {

  def main(args: Array[String]): Unit = {
    // Start Spark session
    val spark = SparkSession.builder()
      .appName("Alt Fuel Stations API Reader")
      .master("local[*]")
      .getOrCreate()

    while (true) {
      import spark.implicits._

      // API details
      val apiUrl = "https://developer.nrel.gov/api/alt-fuel-stations/v1.json?api_key=QadijL96MjkrLZoz3JJmisWblq4fdFv0fbTC7cA5"
      val response = get(apiUrl)
      val jsonResponse = response.text()
      val dfFromText = spark.read.json(Seq(jsonResponse).toDS)


      // Define schema for the full structure
//      val schema = StructType(Seq(
//        StructField("station_locator_url", StringType, true),
//        StructField("total_results", IntegerType, true),
//        StructField("fuel_stations", ArrayType(
//          StructType(Seq(
//            StructField("access_code", StringType, true),
//            StructField("access_days_time", StringType, true),
//            StructField("fuel_type_code", StringType, true),
//            StructField("station_name", StringType, true),
//            StructField("latitude", DoubleType, true),
//            StructField("longitude", DoubleType, true),
//            StructField("city", StringType, true),
//            StructField("state", StringType, true),
//            StructField("street_address", StringType, true),
//            StructField("ev_connector_types", ArrayType(StringType, true), true),
//            StructField("ev_network", StringType, true),
//            StructField("ev_network_web", StringType, true),
//            StructField("ev_pricing", StringType, true),
//            StructField("ev_workplace_charging", BooleanType, true)
//          ))
//        ))
//      ))

//      // Parse JSON response with schema
//      val dfFromJson = spark.read.schema(schema).json(Seq(jsonResponse).toDS)

//      // Explode the fuel_stations array
//      val fuelStationsDF = dfFromJson
//        .select(explode($"fuel_stations").alias("station"))
//        .select(
//          $"station.station_name".alias("station_name"),
//          $"station.fuel_type_code".alias("fuel_type_code"),
//          $"station.latitude".alias("latitude"),
//          $"station.longitude".alias("longitude"),
//          $"station.city".alias("city"),
//          $"station.state".alias("state"),
//          $"station.street_address".alias("street_address"),
//          $"station.ev_connector_types".alias("ev_connector_types"),
//          $"station.ev_network".alias("ev_network"),
//          $"station.ev_network_web".alias("ev_network_web"),
//          $"station.ev_pricing".alias("ev_pricing"),
//          $"station.ev_workplace_charging".alias("ev_workplace_charging")
//        )



      val fuelStationsDF = dfFromText.select($"fuel_stations", $"station.station_name")
      // Show a few rows for debugging
      fuelStationsDF.show(5, truncate = false)

      // Kafka server and topic name assignment
      val kafkaServer: String = "ip-172-31-8-235.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "sujay_topic1" // Your Kafka topic name

      // Write data to Kafka
      fuelStationsDF.selectExpr("CAST(station_name AS STRING) AS key", "to_json(struct(*)) AS value")
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
