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

import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.{StreamingContext, Time}

import scala.reflect.ClassTag

private[streaming]
class RabbitMQDStream[R: ClassTag](
                                    val _ssc: StreamingContext,
                                    val distributedKeys: Seq[RabbitMQDistributedKey],
                                    val rabbitMQParams: Map[String, String],
                                    messageHandler: Array[Byte] => R
                                  ) extends InputDStream[R](_ssc) with Logging {

  private[streaming] override def name: String = s"RabbitMQ direct stream [$id]"

  private val maxRateLimit: Int = context.sparkContext.getConf.getInt(
    "spark.streaming.rabbitmq.maxRateLimit", 0)

  protected[streaming] def maxMessages(): Option[(Int, Long)] = {
    val estimatedRateLimit = rateController.map(_.getLatestRate().toInt)

    estimatedRateLimit.flatMap(rateLimit => {
      if (rateLimit > 0)
        Some((rateLimit, (context.graph.batchDuration.milliseconds.toDouble / 1000 * rateLimit).toLong))
      else None
    })
  }

  override def compute(validTime: Time): Option[RabbitMQRDD[R]] = {
    val rdd = RabbitMQRDD[R](context.sparkContext, distributedKeys, rabbitMQParams, messageHandler)

    // Report the record number and metadata of this batch interval to InputInfoTracker.
    maxMessages().foreach(messages => {
      val description = s"MaxRateLimit : $maxRateLimit\t: Estimated${messages._1}\t Processed: ${messages._2}"
      val metadata = Map(
        "DistributedKeys" -> RabbitMQDistributedKey,
        StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
      val inputInfo = StreamInputInfo(id, rdd.count, metadata)

      ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
    })

    Some(rdd)
  }

  override def start(): Unit = {}

  override def stop(): Unit = {}

  /**
   * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
   */
  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf))
      Some(new DistributedRabbitMQRateController(id, RateEstimator.create(ssc.conf, context.graph.batchDuration)))
    else None
  }

  /**
   * A RateController to retrieve the rate from RateEstimator.
   */
  private[streaming] class DistributedRabbitMQRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {

    override def publish(rate: Long): Unit = ()
  }

}


