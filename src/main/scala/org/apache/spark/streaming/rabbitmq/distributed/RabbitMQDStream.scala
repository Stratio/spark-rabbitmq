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

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.rabbitmq.ConfigParameters._
import org.apache.spark.streaming.rabbitmq.consumer.Consumer
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{Accumulator, Logging}

import scala.reflect.ClassTag

private[streaming]
class RabbitMQDStream[R: ClassTag](
                                    @transient val _ssc: StreamingContext,
                                    val distributedKeys: Seq[RabbitMQDistributedKey],
                                    val rabbitMQParams: Map[String, String],
                                    messageHandler: Array[Byte] => R
                                  ) extends InputDStream[R](_ssc) with Logging {

  override val id = ssc.getNewInputStreamId() + System.currentTimeMillis().toInt

  private[streaming] override def name: String = s"RabbitMQ direct stream [$id]"

  private val maxMessagesPerPartition = Consumer.getMaxMessagesPerPartition(rabbitMQParams)

  storageLevel = calculateStorageLevel()

  /**
   * Remember duration for the rdd created by this InputDstream
   */
  private val userRememberDuration = Consumer.getRememberDuration(rabbitMQParams)
  userRememberDuration.foreach(duration => rememberDuration = Seconds(duration))

  /**
   * Min storage level is memory, because compute function for one rdd is called more than one place
   */
  private[streaming] def calculateStorageLevel(): StorageLevel = {
    val levelFromParams = Consumer.getStorageLevel(rabbitMQParams)
    if (levelFromParams == StorageLevel.NONE) {
      log.info("NONE is not a valid storage level for this receiver distributed, setting it in MEMORY_ONLY")
      StorageLevel.MEMORY_ONLY
    } else levelFromParams
  }

  /**
   * Calculate the max number of messages that the receiver must receive and process in one batch when the
   * blackPressure is enable, then we must override the rabbitMQ property in the rdd creation
   */
  private[streaming] def maxMessages(): Option[(Int, Long)] = {
    val estimatedRateLimit = rateController.map(_.getLatestRate().toInt)
    estimatedRateLimit.flatMap(rateLimit => {
      if (rateLimit > 0) {
        val maxPartitions = getMaxMessagesBasedOnPartitions
        val maxMessages = Math.max(
          (context.graph.batchDuration.milliseconds.toDouble / 1000 * rateLimit).toLong,
          maxPartitions
        )

        Option((rateLimit, maxMessages))
      } else None
    })
  }

  private[streaming] def getMaxMessagesBasedOnPartitions: Long = {
    if (maxMessagesPerPartition.isDefined) {
      val parallelism = Consumer.getParallelism(rabbitMQParams)
      val keys = if (distributedKeys.nonEmpty)
        distributedKeys
      else Consumer.getExchangesDistributed(rabbitMQParams)
      val numPartitions = keys.size * parallelism

      numPartitions * maxMessagesPerPartition.get
    } else 0L
  }

  override def compute(validTime: Time): Option[RabbitMQRDD[R]] = {
    //Set the receive time to the streaming window if is 0 the maxReceiveTime property
    val receiveTime = Consumer.getMaxReceiveTime(rabbitMQParams) match {
      case 0L => context.graph.batchDuration.milliseconds.toString
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
    val countAccumulator = ssc.sparkContext.accumulator(0L, id.toString)
    val rdd = RabbitMQRDD[R](context.sparkContext, distributedKeys, rddParams, countAccumulator, messageHandler)
    val inputInfo = StreamInputInfo(
      RabbitMQDStream.idDstream,
      RabbitMQDStream.countTotalDStream.fold(0L) { acc => acc.value },
      RabbitMQDStream.metadataDStream
    )

    //publish data in Spark UI
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    //Update next values to publish in spark streaming UI
    RabbitMQDStream.countTotalDStream = Option(countAccumulator)
    RabbitMQDStream.metadataDStream = metadata
    RabbitMQDStream.idDstream = id

    Option(rdd)
  }

  override def start(): Unit = {}

  override def stop(): Unit = {}

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
    }
  }

}

private[streaming] object RabbitMQDStream {

  /**
   * Only for report information to Spark UI
   */
  @volatile var countTotalDStream: Option[Accumulator[Long]] = None
  @volatile var metadataDStream: Map[String, Any] = Map.empty[String, Any]
  @volatile var idDstream: Int = 0
}

