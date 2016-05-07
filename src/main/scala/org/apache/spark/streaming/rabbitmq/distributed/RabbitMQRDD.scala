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
package org.apache.spark.streaming.rabbitmq.distributed

import akka.actor.ActorSystem
import com.rabbitmq.client.ConsumerCancelledException
import com.typesafe.config.ConfigFactory
import org.apache.spark.partial.{BoundedDouble, CountEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.rabbitmq.consumer.Consumer
import org.apache.spark.util.{NextIterator, Utils}
import org.apache.spark.{Accumulator, Logging, Partition, SparkContext, SparkException, TaskContext}

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

private[rabbitmq]
class RabbitMQRDD[R: ClassTag](
                                @transient sc: SparkContext,
                                distributedKeys: Seq[RabbitMQDistributedKey],
                                rabbitMQParams: Map[String, String],
                                val countAccumulator: Accumulator[Long],
                                messageHandler: Array[Byte] => R
                              ) extends RDD[R](sc, Nil) with Logging {

  @volatile private var totalCalculated: Option[Long] = None

  /**
   * Return the number of elements in the RDD. Optimized when is the second place
   */
  override def count(): Long = {
    totalCalculated.getOrElse {
      withScope {
        totalCalculated = Option(sc.runJob(this, Utils.getIteratorSize _).sum)
        totalCalculated.get
      }
    }
  }

  /**
   * Return the number of elements in the RDD approximately. Optimized when count are called before
   */
  override def countApprox(
                            timeout: Long,
                            confidence: Double = 0.95): PartialResult[BoundedDouble] = {
    if (totalCalculated.isDefined) {
      val c = count()
      new PartialResult(new BoundedDouble(c, 1.0, c, c), true)
    } else {
      withScope {
        val countElements: (TaskContext, Iterator[R]) => Long = { (ctx, iter) =>
          var result = 0L
          while (iter.hasNext) {
            result += 1L
            iter.next()
          }
          result
        }
        val evaluator = new CountEvaluator(partitions.length, confidence)
        sc.runApproximateJob(this, countElements, evaluator, timeout)
      }
    }
  }

  /**
   * Return if the RDD is empty. Optimized when count are called before
   */
  override def isEmpty(): Boolean = {
    if (totalCalculated.isDefined) {
      count == 0L
    } else {
      withScope {
        partitions.length == 0 || take(1).length == 0
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val parallelism = Consumer.getParallelism(rabbitMQParams)
    val keys = if (distributedKeys.nonEmpty)
      distributedKeys
    else Consumer.getExchangesDistributed(rabbitMQParams)

    keys.zipWithIndex.flatMap { case (key, index) =>
      (0 until parallelism).map(indexParallelism => {
        new RabbitMQPartition(
          parallelism * index + indexParallelism,
          key.queue,
          key.exchangeAndRouting,
          key.connectionParams,
          parallelism > 1
        )
      })
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

    //Listener for control the shutdown process when the tasks are interrupted
    context.addTaskCompletionListener(context => {
      if (context.isInterrupted()) {
        RabbitMQRDD.shutDownActorSystem()
        log.info(s"Task interrupted, closing RabbitMQ connections in partition: ${part.index}")
        closeIfNeeded()
        Consumer.closeConnections()
      }
    })

    val rabbitParams = rabbitMQParams ++ part.connectionParams
    val consumer = getConsumer(part, rabbitParams)
    val queueConsumer = consumer.startConsumer
    @volatile var numMessages = 0
    val system = RabbitMQRDD.getActorSystem
    val receiveTime = Consumer.getMaxReceiveTime(rabbitParams)
    val maxMessagesPerPartition = Consumer.getMaxMessagesPerPartition(rabbitParams)

    //Execute this code every certain time, the consumer must stop with this timeout
    val scheduleProcess = system.scheduler.scheduleOnce(receiveTime milliseconds) {
      finishProcess()
      queueConsumer.handleCancel("timeout")
    }

    log.info(s"Receiving data in Partition ${part.index} from \t[${part.toStringPretty}]")

    override def getNext(): R = {
      synchronized {
        if (finished || (maxMessagesPerPartition.isDefined && numMessages >= maxMessagesPerPartition.get)) {
          finishProcess()
        } else {
          Try {
            val delivery = queueConsumer.nextDelivery()

            (delivery, messageHandler(delivery.getBody))
          } match {
            case Success((delivery, data)) =>
              if (Consumer.sendingBasicAckFromParams(rabbitParams))
                consumer.sendBasicAck(delivery)
              numMessages += 1
              data
            case Failure(e: ConsumerCancelledException) =>
              finishProcess()
            case Failure(e) =>
              Try {
                finishProcess()
                closeIfNeeded()
              }
              throw new SparkException(s"Error receiving data from RabbitMQ with error: ${e.getLocalizedMessage}")
          }
        }
      }
    }

    override def close(): Unit = {
      countAccumulator += numMessages
      log.info(s"******* Received $numMessages messages by Partition : ${part.index}  before close Channel ******")
      scheduleProcess.cancel()
      consumer.close()
    }

    private def finishProcess(): R = {
      finished = true
      null.asInstanceOf[R]
    }

    private def getConsumer(part: RabbitMQPartition, consumerParams: Map[String, String]): Consumer = {
      val consumer = Consumer(consumerParams)
      consumer.setQueue(
        part.queue,
        part.exchangeAndRouting.exchangeName,
        part.exchangeAndRouting.exchangeType,
        part.exchangeAndRouting.routingKeys,
        consumerParams
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
                         countAccumulator: Accumulator[Long],
                         messageHandler: Array[Byte] => R
                        ): RabbitMQRDD[R] = {

    new RabbitMQRDD[R](sc, distributedKeys, rabbitMQParams, countAccumulator, messageHandler)
  }

  def getActorSystem: ActorSystem = {
    synchronized {
      if (system.isEmpty || (system.isDefined && system.get.isTerminated))
        system = Option(akka.actor.ActorSystem(
          s"system-${System.currentTimeMillis()}",
          ConfigFactory.load(ConfigFactory.parseString("akka.daemonic=on"))
        ))
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
