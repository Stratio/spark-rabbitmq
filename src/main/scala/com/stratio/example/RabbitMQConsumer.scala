package com.stratio.example

import com.stratio.receiver.RabbitMQUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by vagrant on 9/1/15.
 */
object RabbitMQConsumer {
  def main(args: Array[String]) {
    // Setup the Streaming context
    val conf = new SparkConf()
      .setAppName("rabbitmq-receiver-example")
      .setIfMissing("spark.master", "local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))

    // Setup the SQL context
    val sqlContext = new SQLContext(ssc.sparkContext)

    // Setup the receiver stream to connect to RabbitMQ.
    // Check the RabbitMQInputDStream class to see the full list of
    // options, along with the default values.
    val receiverStream = RabbitMQUtils.createStream(ssc, Map(
      "host" -> "localhost",
      "queueName" -> "rabbitmq-queue",
      "exchangeName" -> "rabbitmq-exchange",
      "vHost" -> "rabbitmq-vHost",
      "username" -> "rabbitmq-user",
      "password" -> "rabbitmq-password"
    ))

    // Start up the receiver.
    receiverStream.start()

    // Fires each time the configured window has passed.
    receiverStream.foreachRDD(r => {
      if (r.count() > 0) {
        // Do something with this message
        println(r)
      }
      else {
        println("No new messages...")
      }
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
