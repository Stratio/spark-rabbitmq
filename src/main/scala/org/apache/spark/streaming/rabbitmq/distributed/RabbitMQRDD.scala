/**
 * Copyright (C) 2016 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.rabbitmq.distributed

import com.rabbitmq.client.QueueingConsumer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.rabbitmq.Consumer
import org.apache.spark.util.NextIterator
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

private[rabbitmq]
class RabbitMQRDD[R: ClassTag](
                                sc: SparkContext,
                                distributedKeys: Seq[RabbitMQDistributedKey],
                                rabbitMQParams: Map[String, String],
                                messageHandler: Array[Byte] => R
                              ) extends RDD[R](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    val keys = if (distributedKeys.nonEmpty)
      distributedKeys
    else Consumer.getExchangesDistributed(rabbitMQParams)
    val locations = Consumer.getHosts(rabbitMQParams)

    keys.zipWithIndex.map { case (key, index) =>
      new RabbitMQPartition(
        index,
        key.queue,
        key.exchangeName,
        key.exchangeType,
        key.routingKey,
        locations,
        keys.size > 1
      )
    }.toArray
  }

  override def getPreferredLocations(thePart: Partition): Seq[String] = {
    val part = thePart.asInstanceOf[RabbitMQPartition]

    Seq(part.toString)
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[R] =
    new RabbitMQRDDIterator(thePart.asInstanceOf[RabbitMQPartition], context)

  private class RabbitMQRDDIterator(
                                     part: RabbitMQPartition,
                                     context: TaskContext) extends NextIterator[R] {

    context.addTaskCompletionListener { context => closeIfNeeded() }

    log.info(s"Computing queue ${part.queue}, exchange ${part.exchangeName.getOrElse("")}, exchangeType " +
      s"${part.exchangeType.getOrElse("")} addresses ${part.addresses}")

    val consumer = Consumer(rabbitMQParams)

    consumer.setQueue(part.queue, part.exchangeName, part.exchangeType, part.routingKeys, rabbitMQParams)

    if (part.withFairDispatch)
      consumer.setFairDispatchQoS()

    val queueConsumer: QueueingConsumer = consumer.startConsumer
    var numMessages = 0

    override def getNext(): R = {
      if (numMessages < Consumer.getMaxMessagesPerPartition(rabbitMQParams)) {
        val delivery = queueConsumer.nextDelivery()

        numMessages += 1
        messageHandler(delivery.getBody)
      } else {
        numMessages = 0
        finished = true
        null.asInstanceOf[R]
      }
    }

    override def close(): Unit = consumer.close()
  }

}

private[rabbitmq]
object RabbitMQRDD {

  def apply[R: ClassTag](sc: SparkContext,
                         distributedKeys: Seq[RabbitMQDistributedKey],
                         rabbitMQParams: Map[String, String],
                         messageHandler: Array[Byte] => R
                        ): RabbitMQRDD[R] = {

    new RabbitMQRDD[R](sc, distributedKeys, rabbitMQParams, messageHandler)
  }
}
