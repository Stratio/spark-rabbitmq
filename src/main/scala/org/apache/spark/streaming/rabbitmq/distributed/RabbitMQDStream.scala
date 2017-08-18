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

import com.rabbitmq.client.QueueingConsumer.Delivery
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.rabbitmq.ConfigParameters._
import org.apache.spark.streaming.rabbitmq.consumer.Consumer
import org.apache.spark.streaming.rabbitmq.distributed.RabbitMQDStream._
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.util.LongAccumulator

import scala.reflect.ClassTag

private[streaming]
class RabbitMQDStream[R: ClassTag](
                                    @transient val _ssc: StreamingContext,
                                    val distributedKeys: Seq[RabbitMQDistributedKey],
                                    val rabbitMQParams: Map[String, String],
                                    messageHandler: Delivery => R
                                  ) extends InputDStream[R](_ssc) with Logging {

  private[streaming] override def name: String = s"RabbitMQ direct stream [$id]"

  storageLevel = calculateStorageLevel()

  /**
   * Remember duration for the rdd created by this InputDStream,
   * by default DefaultMinRememberDuration = 60s * slideWindow
   */
  private val userRememberDuration = Consumer.getRememberDuration(rabbitMQParams)

  userRememberDuration match {
    case Some(duration) =>
      remember(Seconds(duration))
    case None =>
      val minRememberDuration = Seconds(ssc.conf.getTimeAsSeconds(
        ssc.conf.get("spark.streaming.minRememberDuration", DefaultMinRememberDuration), DefaultMinRememberDuration))
      val numBatchesToRemember = math.ceil(minRememberDuration.milliseconds / slideDuration.milliseconds).toInt

      remember(slideDuration * numBatchesToRemember)
  }

  private val maxMessagesPerPartition = Consumer.getMaxMessagesPerPartition(rabbitMQParams)

  /**
   * Min storage level is MEMORY_ONLY, because compute function for one rdd is called more than one place
   */
  private[streaming] def calculateStorageLevel(): StorageLevel = {
    val levelFromParams = Consumer.getStorageLevel(rabbitMQParams)
    if (levelFromParams == StorageLevel.NONE) {
      log.warn("NONE is not a valid storage level for this receiver distributed, setting it in MEMORY_ONLY")
      StorageLevel.MEMORY_ONLY
    } else levelFromParams
  }

  /**
   * Calculate the max number of messages that the receiver must receive and process in one batch when the
   * backPressure is enable, then we must override the rabbitMQ property in the rdd creation
   */
  private[streaming] def maxMessages(): Option[(Int, Long)] = {
    val estimatedRateLimit = rateController.map(_.getLatestRate().toInt)
    estimatedRateLimit.flatMap(estimatedRateLimit => {
      if (estimatedRateLimit > 0) {
        val messagesRateController = ((slideDuration.milliseconds.toDouble / 1000) * estimatedRateLimit).toLong

        Option((estimatedRateLimit, getMaxMessagesBasedOnPartitions(messagesRateController)))
      } else None
    })
  }

  /**
   *
   * @return max number of messages that the input RDD must receive in the next window by partition based on the
   *         parallelism multiplied with the number of distributed keys or the number of exchanges
   */
  private[streaming] def getMaxMessagesBasedOnPartitions(messagesRateController: Long): Long = {
    maxMessagesPerPartition.fold(getNumPartitions * messagesRateController) { maxMessagesPartition =>
      Math.min(messagesRateController, maxMessagesPartition)
    }
  }

  /**
   * @return The number of partitions for the RDD created by this DStream
   */
  private[streaming] def getNumPartitions: Int = {
    val parallelism = Consumer.getParallelism(rabbitMQParams)
    val keys = if (distributedKeys.nonEmpty)
      distributedKeys
    else Consumer.getDistributedKeysParams(rabbitMQParams)

    keys.size * parallelism
  }

  override def compute(validTime: Time): Option[RabbitMQRDD[R]] = {
    //Set the receive time to the streaming window if is 0 the maxReceiveTime property
    val receiveTime = Consumer.getMaxReceiveTime(rabbitMQParams) match {
      case 0L => math.ceil(slideDuration.milliseconds * DefaultRateReceiveCompute).toInt.toString
      case value: Long => value.toString
    }
    // Report the record number and metadata of this batch interval to InputInfoTracker and calculate the maxMessages
    val maxMessagesCalculation = maxMessages().map { case (estimated, newMaxMessages) =>
      val description = s"MaxMessagesPerPartition : ${maxMessagesPerPartition.getOrElse("")}\t:" +
        s" Estimated: $estimated\t NewMaxMessages: $newMaxMessages"

      (StreamInputInfo.METADATA_KEY_DESCRIPTION -> description, newMaxMessages)
    }
    val metadata = Map(
      "DistributedKeys" -> RabbitMQDistributedKey,
      "ReceiveTimeout" -> (System.currentTimeMillis() + receiveTime.toLong)
    ) ++ maxMessagesCalculation.fold(Map.empty[String, Any]) { case (description, _) => Map(description) }
    val rddParams = rabbitMQParams ++
      Map(MaxReceiveTime -> receiveTime) ++
      maxMessagesCalculation.fold(Map.empty[String, String]) { case (_, maxMessages) =>
        Map(MaxMessagesPerPartition -> maxMessages.toString)
      }
    val countAccumulator: LongAccumulator = ssc.sparkContext.longAccumulator(id.toString)
    val rdd = RabbitMQRDD[R](context.sparkContext, distributedKeys, rddParams, countAccumulator, messageHandler)

    //publish data in Spark UI
    countTotalDStream.foreach(countTotal =>
      ssc.scheduler.inputInfoTracker.reportInfo(validTime, StreamInputInfo(id, countTotal.value, metadataDStream))
    )
    //Update next values to publish in spark streaming UI
    countTotalDStream = Option(countAccumulator)
    metadataDStream = metadata

    Option(rdd)
  }

  override def start(): Unit = {}

  override def stop(): Unit = {
    RabbitMQRDD.shutDownActorSystem()
    Consumer.closeConnections()
  }

  /**
   * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
   */
  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf))
      Option(new DistributedRabbitMQRateController(id, RateEstimator.create(ssc.conf, context.graph.batchDuration)))
    else None
  }

  /**
   * A RateController to retrieve the rate from RateEstimator.
   */
  private[streaming] class DistributedRabbitMQRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {

    override def publish(rate: Long): Unit = {
      ssc.scheduler.receiverTracker.sendRateUpdate(id, rate)
    }
  }

}

private[streaming] object RabbitMQDStream {

  /**
   * Only for report information to Spark UI
   */
  @volatile var countTotalDStream: Option[LongAccumulator] = None
  @volatile var metadataDStream: Map[String, Any] = Map.empty[String, Any]
}

