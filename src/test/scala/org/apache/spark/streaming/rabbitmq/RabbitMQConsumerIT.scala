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

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RabbitMQConsumerIT extends TemporalDataSuite {

  override val queueName = s"$configQueueName-${this.getClass().getName()}-${UUID.randomUUID().toString}"

  override val exchangeName = s"$configExchangeName-${this.getClass().getName()}-${UUID.randomUUID().toString}"

  test("RabbitMQ Receiver should read all the records") {

    val receiverStream = RabbitMQUtils.createStream(ssc, Map(
      "hosts" -> hosts,
      "queueName" -> queueName,
      "exchangeName" -> exchangeName,
      "exchangeType" -> exchangeType,
      "vHost" -> vHost,
      "userName" -> userName,
      "password" -> password
    ))
    val totalEvents = ssc.sparkContext.longAccumulator("My Accumulator")

    // Start up the receiver.
    receiverStream.start()

    // Fires each time the configured window has passed.
    receiverStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val count = rdd.count()
        // Do something with this message
        println(s"EVENTS COUNT : \t $count")
        totalEvents.add(count)
        //rdd.collect().sortBy(event => event.toInt).foreach(event => print(s"$event, "))
      } else println("RDD is empty")
      println(s"TOTAL EVENTS : \t $totalEvents")
    })

    ssc.start() // Start the computation
    ssc.awaitTerminationOrTimeout(10000L) // Wait for the computation to terminate

    assert(totalEvents.value === totalRegisters.toLong)
  }
}
