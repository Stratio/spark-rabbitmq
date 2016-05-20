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

import org.apache.spark.Partition
import org.apache.spark.streaming.rabbitmq.models.ExchangeAndRouting

private[rabbitmq]
class RabbitMQPartition(
                         val index: Int,
                         val queue: String,
                         val exchangeAndRouting: ExchangeAndRouting,
                         val connectionParams: Map[String, String],
                         val withFairDispatch: Boolean
                       ) extends Partition {

  override def toString: String = s"${index.toString},$queue, ${exchangeAndRouting.toString()}," +
    s" ${connectionParams.mkString(" , ")}, ${withFairDispatch.toString}"

  def toStringPretty: String = s"queue: $queue, ${exchangeAndRouting.toStringPretty()}, connectionParams: " +
    s"${connectionParams.mkString(" , ")}, withFairDispatch: $withFairDispatch"
}
