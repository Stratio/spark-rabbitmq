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
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils
import org.apache.spark.streaming.rabbitmq.distributed.RabbitMQDistributedKey
import org.apache.spark.streaming.rabbitmq.models.ExchangeAndRouting
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RabbitMQDistributedConsumer {

  def main(args: Array[String]) {
    // Setup the Streaming context
    val conf = new SparkConf()
      .setAppName("rabbitmq-receiver-example")
      .setIfMissing("spark.master", "local[1]")
    val ssc = new StreamingContext(conf, Seconds(10))

    val rabbitMQParams = Map(
      "hosts" -> "localhost",
      "queueName" -> "rabbitmq-queue",
      "exchangeName" -> "rabbitmq-exchange",
      "vHost" -> "/",
      "username" -> "guest",
      "password" -> "guest",
      //"maxMessagesPerPartition" -> "1000",
      "levelParallelism" -> "1",
      //"ackType" -> "basic",
      "maxReceiveTime" -> "9000"
    )
    val distributedKey = Seq(RabbitMQDistributedKey(
      "rabbitmq-queue",
      new ExchangeAndRouting("rabbitmq-exchange", "rabbitmq-queue"),
      rabbitMQParams
    ))
    val distributedStream = RabbitMQUtils.createDistributedStream[String](ssc, distributedKey, rabbitMQParams)

    val totalEvents = ssc.sparkContext.accumulator(0L, "My Accumulator")

    // Start up the receiver.
    distributedStream.start()

    // Fires each time the configured window has passed.
    distributedStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val count = rdd.count()
        val count2 = rdd.count()
        // Do something with this message
        println(s"EVENTS COUNT : \t $count")
        println(s"EVENTS COUNT2 : \t $count2")
        totalEvents += count
      } else println("RDD is empty")
      println(s"TOTAL EVENTS : \t $totalEvents")
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}

