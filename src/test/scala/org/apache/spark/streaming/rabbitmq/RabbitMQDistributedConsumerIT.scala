/*
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
package org.apache.spark.streaming.rabbitmq

import java.util.UUID

import com.rabbitmq.client.QueueingConsumer.Delivery
import org.apache.spark.streaming.rabbitmq.distributed.RabbitMQDistributedKey
import org.apache.spark.streaming.rabbitmq.models.ExchangeAndRouting
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RabbitMQDistributedConsumerIT extends TemporalDataSuite {

  override val queueName = s"$configQueueName-${this.getClass().getName()}-${UUID.randomUUID().toString}"

  override val exchangeName = s"$configExchangeName-${this.getClass().getName()}-${UUID.randomUUID().toString}"

    test("RabbitMQ Receiver should read all the records") {
      /**
       * Is possible to use this params example:
       * Map(
       * "maxMessagesPerPartition" -> "1000",
       * "storageLevel" -> "MEMORY_AND_DISK",
       * "ackType" -> "auto",
       * "maxReceiveTime" -> "9000",
       * "rememberDuration" -> "20000",
       * "levelParallelism" -> "1"
       * )
       */
      val rabbitMQParams = Map.empty[String, String]

      val rabbitMQConnection = Map(
        "hosts" -> hosts,
        "queueName" -> queueName,
        "exchangeName" -> exchangeName,
        "vHost" -> vHost,
        "userName" -> userName,
        "password" -> password
      )

      val distributedKey = Seq(
        RabbitMQDistributedKey(
          queueName,
          new ExchangeAndRouting(exchangeName, routingKey),
          rabbitMQConnection
        )
      )

      //Delivery is not Serializable by Spark, is possible use Map, Seq or native Classes
      import scala.collection.JavaConverters._
      val distributedStream = RabbitMQUtils.createDistributedStream[Map[String, Any]](
        ssc,
        distributedKey,
        rabbitMQParams,
        (rawMessage: Delivery) =>
          Map(
            "body" -> new Predef.String(rawMessage.getBody),
            "exchange" -> rawMessage.getEnvelope.getExchange,
            "routingKey" -> rawMessage.getEnvelope.getRoutingKey,
            "deliveryTag" -> rawMessage.getEnvelope.getDeliveryTag
          ) ++ {
            //Avoid null pointer Exception
            Option(rawMessage.getProperties.getHeaders) match {
              case Some(headers) => Map("headers" -> headers.asScala)
              case None => Map.empty[String, Any]
            }
          }
      )

      val totalEvents = ssc.sparkContext.longAccumulator("Number of events received")

      // Start up the receiver.
      distributedStream.start()

      // Fires each time the configured window has passed.
      distributedStream.foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          val count = rdd.count()
          // Do something with this message
          println(s"EVENTS COUNT : \t $count")
          totalEvents.add(count)
          //rdd.collect().foreach(event => print(s"${event.toString}, "))
        } else println("RDD is empty")
        println(s"TOTAL EVENTS : \t $totalEvents")
      })

      ssc.start() // Start the computation
      ssc.awaitTerminationOrTimeout(10000L) // Wait for the computation to terminate

      assert(totalEvents.value === totalRegisters.toLong)
    }
}

