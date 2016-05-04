package com.stratio.receiver

import org.apache.spark.SparkConf
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils
import org.apache.spark.streaming.rabbitmq.distributed.RabbitMQDistributedKey
import org.apache.spark.streaming.rabbitmq.models.ExchangeAndRouting
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RabbitMQDistributedConsumer {

  def main(args: Array[String]) {
    // Setup the Streaming context
    val conf = new SparkConf()
      .setAppName("rabbitmq-receiver-example")
      .setIfMissing("spark.master", "local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val rabbitMQParams = Map(
      "hosts" -> "localhost",
      "queueName" -> "rabbitmq-queue",
      "exchangeName" -> "rabbitmq-exchange",
      "vHost" -> "/",
      "username" -> "guest",
      "password" -> "guest",
      "maxMessagesPerPartition" -> "5",
      "levelParallelism" -> "1"
    )
    val distributedKey = Seq(RabbitMQDistributedKey(
      "rabbitmq-queue",
      new ExchangeAndRouting("rabbitmq-exchange", "rabbitmq-queue"),
      rabbitMQParams
    ))
    val receiverStream = RabbitMQUtils.createDistributedStream[String](ssc, distributedKey, rabbitMQParams)

    // Start up the receiver.
    receiverStream.start()

    // Fires each time the configured window has passed.
    receiverStream.foreachRDD(r => {
      val count = r.count()
      if (count > 0) {
        // Do something with this message
        println(s"DSTREAM COUNT : \t $count")
      }
      else {
        println("No new messages...")
      }
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
