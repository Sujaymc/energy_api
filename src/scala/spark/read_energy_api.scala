package spark // Change to your directory name

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import requests._

object read_energy_api {

  def main(args: Array[String]): Unit = {
    // Start Spark session
    val spark = SparkSession.builder()
      .appName("Electricity Production API Reader")
      .master("local[*]")
      .getOrCreate()

    while (true) {
      import spark.implicits._

      // API details loading
      val apiUrl = "https://global-electricity-production.p.rapidapi.com/country"
      val querystring = Map("country" -> "Afghanistan")
      val headers = Map(
        "x-rapidapi-key" -> "48bce046c0mshee45259ba8a9955p1a871bjsn90680f9cecd6",
        "x-rapidapi-host" -> "global-electricity-production.p.rapidapi.com"
      )

      val response = get(apiUrl, headers = headers, params = querystring)
      val total = response.text()
      val dfFromText = spark.read.json(Seq(total).toDS)

      // Select the columns you want to include in the message
      val messageDF = dfFromText.select(
        $"country",
        $"code",
        $"year",
        $"coal",
        $"gas",
        $"hydro",
        $"other_renewables",
        $"solar",
        $"oil",
        $"wind",
        $"nuclear"
      )

      // Show a few messages, e.g., 5 rows
      messageDF.show(5, truncate = false)

      // Kafka server and topic name assignment
      val kafkaServer: String = "ip-172-31-8-235.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "sujay_energy" // Your Kafka topic name

      // Write data to Kafka
      messageDF.selectExpr("CAST(country AS STRING) AS key", "to_json(struct(*)) AS value")
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
//spark-submit --master local --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7","com.lihaoyi:requests_2.11:0.7.1" --class spark.sop_read_api target/ElectricityAPIReader-1.0-SNAPSHOT.jar
