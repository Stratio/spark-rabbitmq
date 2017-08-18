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
package org.apache.spark.streaming.rabbitmq.distributed

import akka.actor.ActorSystem
import com.rabbitmq.client.ConsumerCancelledException
import com.rabbitmq.client.QueueingConsumer.Delivery
import com.typesafe.config.ConfigFactory
import org.apache.spark.internal.Logging
import org.apache.spark.partial.{BoundedDouble, CountEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.rabbitmq.consumer.Consumer
import org.apache.spark.streaming.rabbitmq.consumer.Consumer._
import org.apache.spark.util.{LongAccumulator, NextIterator, Utils}
import org.apache.spark.{Partition, SparkContext, SparkException, TaskContext}

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

private[rabbitmq]
class RabbitMQRDD[R: ClassTag](
                                @transient sc: SparkContext,
                                distributedKeys: Seq[RabbitMQDistributedKey],
                                rabbitMQParams: Map[String, String],
                                val countAccumulator: LongAccumulator,
                                messageHandler: Delivery => R
                              ) extends RDD[R](sc, Nil) with Logging {

  @volatile private var totalCalculated: Option[Long] = None

  /**
   * Return the number of elements in the RDD. Optimized when is called the second place
   */
  override def count(): Long = {
    totalCalculated.getOrElse {
      withScope {
        sc.runJob(this, Utils.getIteratorSize _)
        totalCalculated = Option(countAccumulator.value)
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
    totalCalculated.fold {
      withScope {
        partitions.length == 0 || take(1).length == 0
      }
    } { total => total == 0L }
  }

  override def take(num: Int): Array[R] = {
    if (totalCalculated.isEmpty) count()
    super.take(num)
  }

  /**
   * The number of partitions are calculated in base of the selected parallelism multiplied by the number of RabbitMQ
   * connections selected by the user when create the DStream, if the sequence of distributed keys is empty, it is
   * multiplied by the distributedKey calculate in base of the params.
   *
   * @return the number of Partitions calculated for this RDD
   */
  override def getPartitions: Array[Partition] = {
    val parallelism = getParallelism(rabbitMQParams)
    val keys = if (distributedKeys.nonEmpty)
      distributedKeys
    else getDistributedKeysParams(rabbitMQParams)

    keys.zipWithIndex.flatMap { case (key, index) =>
      (0 until parallelism).map(indexParallelism => {
        new RabbitMQPartition(
          parallelism * index + indexParallelism,
          key.queue,
          key.exchangeAndRouting,
          rabbitMQParams ++ key.connectionParams,
          parallelism > 1
        )
      })
    }.toArray
  }

  /**
   * The Preferred locations are calculate in base of the hosts when the partition was created
   *
   * @param thePart Partition to calculate the locations
   * @return The sequence of locations
   */
  override def getPreferredLocations(thePart: Partition): Seq[String] = {
    val part = thePart.asInstanceOf[RabbitMQPartition]

    Seq(getHosts(part.connectionParams))
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[R] = {
    val rabbitMQPartition = thePart.asInstanceOf[RabbitMQPartition]

    log.debug(s"Computing Partition: ${thePart.index} from \t[${rabbitMQPartition.toStringPretty}]")

    new RabbitMQRDDIterator(rabbitMQPartition, context)
  }

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
        closeConnections()
      }
    })

    //Parameters of the RDD are merged with the parameters for this partition
    val rabbitParams = part.connectionParams
    //Get or create one consumer, create one new channel if this consumer use one connection that was created
    // previously is reused
    val consumer = getConsumer(part, rabbitParams)
    val queueConsumer = consumer.startConsumer
    //Counter to control the number of messages consumed by this partition
    @volatile var numMessages = 0

    //The actorSystem and the receiveTime are used to limit the number of milliseconds that the partition is
    // receiving data from RabbitMQ
    val system = RabbitMQRDD.getActorSystem
    val receiveTime = getMaxReceiveTime(rabbitParams)
    val maxMessagesPerPartition = getMaxMessagesPerPartition(rabbitParams)

    //Execute this code every certain time, the consumer must stop with this timeout
    val scheduleProcess = system.scheduler.scheduleOnce(receiveTime milliseconds) {
      finished = true
      queueConsumer.handleCancel("timeout")
    }

    log.info(s"Receiving data in Partition ${part.index} from \t[${part.toStringPretty}]")

    override def getNext(): R = {
      synchronized {
        if (finished || (maxMessagesPerPartition.isDefined && numMessages >= maxMessagesPerPartition.get)) {
          finishIterationAndReturn()
        } else {
          Try(queueConsumer.nextDelivery())
          match {
            case Success(delivery) =>
              processDelivery(delivery)
            case Failure(e: ConsumerCancelledException) =>
              finishIterationAndReturn()
            case Failure(e) =>
              Try {
                finished = true
                closeIfNeeded()
              }
              throw new SparkException(s"Error receiving data from RabbitMQ with error: ${e.getLocalizedMessage}", e)
          }
        }
      }
    }

    private def processDelivery(delivery: Delivery): R = {
      Try(messageHandler(delivery))
      match {
        case Success(data) =>
          //Send ack if not set the auto ack property
          if (sendingBasicAckFromParams(rabbitParams))
            consumer.sendBasicAck(delivery)
          //Increment the number of messages consumed correctly
          numMessages += 1
          data
        case Failure(e) =>
          //Send noack if not set the auto ack property
          if (sendingBasicAckFromParams(rabbitParams)) {
            log.warn(s"failed to process message. Sending noack ...", e)
            consumer.sendBasicNAck(delivery)
          }
          null.asInstanceOf[R]
      }
    }

    override def close(): Unit = {
      //Increment the accumulator to control in the driver the number of messages consumed by all executors, this is
      // used to report in the Spark UI this number for the next iteration
      countAccumulator.add(numMessages)
      log.info(s"******* Received $numMessages messages by Partition : ${part.index}  before close Channel ******")
      //Close the scheduler and the channel in the consumer
      scheduleProcess.cancel()
      consumer.close()
    }

    private def finishIterationAndReturn(): R = {
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
      //If the number of consumers in the same queue are more than one, the Fair Dispatch should be 1, in other case
      // the user can lose events
      if (getFairDispatchFromParams(consumerParams))
        consumer.setFairDispatchQoS(getPrefetchCountFromParams(consumerParams))

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
                         countAccumulator: LongAccumulator,
                         messageHandler: Delivery => R
                        ): RabbitMQRDD[R] = {

    new RabbitMQRDD[R](sc, distributedKeys, rabbitMQParams, countAccumulator, messageHandler)
  }

  def getActorSystem: ActorSystem = {
    synchronized {
      if (system.isEmpty || (system.isDefined && system.get.whenTerminated.isCompleted))
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
        actorSystem.terminate()
        system = None
      })
    }
  }
}
