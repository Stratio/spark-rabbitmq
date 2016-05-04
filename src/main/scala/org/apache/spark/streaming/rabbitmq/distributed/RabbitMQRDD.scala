/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
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

import akka.actor.ActorSystem
import com.rabbitmq.client.ConsumerCancelledException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.rabbitmq.consumer.Consumer
import org.apache.spark.util.NextIterator
import org.apache.spark.{Logging, Partition, SparkContext, SparkException, TaskContext}

import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

private[rabbitmq]
class RabbitMQRDD[R: ClassTag](
                                sc: SparkContext,
                                distributedKeys: Seq[RabbitMQDistributedKey],
                                rabbitMQParams: Map[String, String],
                                messageHandler: Array[Byte] => R
                              ) extends RDD[R](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    val parallelism = Consumer.getParallelism(rabbitMQParams)
    val keys = if (distributedKeys.nonEmpty)
      distributedKeys
    else Consumer.getExchangesDistributed(rabbitMQParams)

    keys.zipWithIndex.flatMap { case (key, index) =>
      (0 until parallelism).map(indexParallelism =>
        new RabbitMQPartition(
          parallelism * index + indexParallelism,
          key.queue,
          key.exchangeAndRouting,
          key.connectionParams,
          parallelism > 1
        ))
    }.toArray
  }

  override def getPreferredLocations(thePart: Partition): Seq[String] = {
    val part = thePart.asInstanceOf[RabbitMQPartition]

    Seq(Consumer.getHosts(part.connectionParams))
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[R] =
    new RabbitMQRDDIterator(thePart.asInstanceOf[RabbitMQPartition], context)

  private class RabbitMQRDDIterator(
                                     part: RabbitMQPartition,
                                     context: TaskContext) extends NextIterator[R] {

    import system.dispatcher

    val rabbitParams = rabbitMQParams ++ part.connectionParams
    val consumer = getConsumer(part, rabbitParams)
    val queueConsumer = consumer.startConsumer
    var numMessages = 0
    val system = RabbitMQRDD.getActorSystem
    val receiveTime = Consumer.getReceiveTime(rabbitMQParams)

    log.info(s"Receiving data in Partition ${part.index} from \t[${part.toStringPretty}]")

    context.addTaskCompletionListener(context => {
      if (context.isInterrupted()) {
        RabbitMQRDD.shutDownActorSystem()
        log.info(s"Task interrupted, closing RabbitMQ connections in partition: ${part.index}")
        closeIfNeeded()
      }
    })

    system.scheduler.scheduleOnce(receiveTime milliseconds) {
      log.debug(s"Received $numMessages messages by Partition : ${part.index}")
      finished = true
      queueConsumer.handleCancel("timeout")
    }

    override def getNext(): R = {
      if (!finished || numMessages < Consumer.getMaxMessagesPerPartition(rabbitMQParams)) {
        Try {
          val delivery = queueConsumer.nextDelivery()

          if (Consumer.sendingBasicAckFromParams(rabbitMQParams))
            consumer.sendBasicAck(delivery)

          numMessages += 1
          messageHandler(delivery.getBody)
        } match {
          case Success(data) =>
            data
          case Failure(e: ConsumerCancelledException) =>
            finishProcess()
          case Failure(e) =>
            throw new SparkException(s"Error receiving data from RabbitMQ with error: ${e.getLocalizedMessage}")
        }
      } else finishProcess()
    }

    override def close(): Unit = consumer.close()

    private def finishProcess(): R = {
      finished = true
      null.asInstanceOf[R]
    }

    private def getConsumer(part: RabbitMQPartition, rabbitParams: Map[String, String]): Consumer = {
      val consumer = Consumer(rabbitParams)
      consumer.setQueue(
        part.queue,
        part.exchangeAndRouting.exchangeName,
        part.exchangeAndRouting.exchangeType,
        part.exchangeAndRouting.routingKeys,
        rabbitParams
      )
      if (part.withFairDispatch)
        consumer.setFairDispatchQoS()

      consumer
    }
  }

}

private[rabbitmq]
object RabbitMQRDD extends Logging {

  @volatile private var system: Option[ActorSystem] = None

  def apply[R: ClassTag](sc: SparkContext,
                         distributedKeys: Seq[RabbitMQDistributedKey],
                         rabbitMQParams: Map[String, String],
                         messageHandler: Array[Byte] => R
                        ): RabbitMQRDD[R] = {

    new RabbitMQRDD[R](sc, distributedKeys, rabbitMQParams, messageHandler)
  }

  def getActorSystem: ActorSystem = {
    synchronized {
      if (system.isEmpty || (system.isDefined && system.get.isTerminated))
        system = Option(akka.actor.ActorSystem(s"system-${System.currentTimeMillis()}"))
      system.get
    }
  }

  def shutDownActorSystem(): Unit = {
    synchronized {
      system.foreach(actorSystem => {
        log.debug(s"Shutting down actor system: ${actorSystem.name}")
        actorSystem.shutdown()
      })
    }
  }
}
