/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.receiver

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
    // All the parameters are shown below, remove the ones
    // that you don't need
    val receiverStream = RabbitMQUtils.createStream(ssc, Map(
      "hosts" -> "localhost",
      "queueName" -> "rabbitmq-queue",
      "exchangeName" -> "rabbitmq-exchange",
      "vHost" -> "/",
      "username" -> "guest",
      "password" -> "guest"
    ))
    val extraParams = Map(
      "x-max-length" -> "value",
      "x-max-length" -> "value",
      "x-message-ttl" -> "value",
      "x-expires" -> "value",
      "x-max-length-bytes" -> "value",
      "x-dead-letter-exchange" -> "value",
      "x-dead-letter-routing-key" -> "value",
      "x-max-priority" -> "value"
    )

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
