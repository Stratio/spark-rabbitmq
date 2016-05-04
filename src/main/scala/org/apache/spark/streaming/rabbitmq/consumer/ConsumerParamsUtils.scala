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
package org.apache.spark.streaming.rabbitmq.consumer

import com.rabbitmq.client.Address
import org.apache.spark.streaming.rabbitmq.ConfigParameters._
import org.apache.spark.streaming.rabbitmq.distributed.RabbitMQDistributedKey
import org.apache.spark.streaming.rabbitmq.models.{ExchangeAndRouting, QueueConnectionOpts}

import scala.util.Try

private[rabbitmq]
trait ConsumerParamsUtils {

  def getReceiveTime(params: Map[String, String]): Long =
    Try(params.getOrElse(ReceiveTime, DefaultReceiveTime.toString).toLong)
      .getOrElse(DefaultReceiveTime)

  def getExchangesDistributed(params: Map[String, String]): Seq[RabbitMQDistributedKey] = {
    val queueName = params.get(QueueNameKey)
    val connectionParams = getConnectionParams(params)

    queueName.fold(Seq.empty[RabbitMQDistributedKey]) { queue => {
      val exchangeAndRouting = getExchangeAndRoutingParams(params)

      Seq(RabbitMQDistributedKey(queue, exchangeAndRouting, connectionParams))
    }
    }
  }

  def getAutoAckFromParams(params: Map[String, String]): Boolean = {
    params.getOrElse(AckTypeKey, DefaultAckType) match {
      case DefaultAckType => true
      case _ => false
    }
  }

  def getAckFromParams(params: Map[String, String]): String =
    params.getOrElse(AckTypeKey, DefaultAckType)

  def sendingBasicAckFromParams(params: Map[String, String]): Boolean =
    getAckFromParams(params) == BasicAckType

  def getExchangeAndRoutingParams(params: Map[String, String]): ExchangeAndRouting = {
    val routingKeys = params.get(RoutingKeysKey)
    val exchangeName = params.get(ExchangeNameKey)
    val exchangeType = params.get(ExchangeTypeKey)

    ExchangeAndRouting(exchangeName, exchangeType, routingKeys)
  }

  def getConnectionParams(params: Map[String, String]): Map[String, String] =
    params.filterKeys(key => ConnectionKeys.contains(key))

  def getHosts(params: Map[String, String]): String = {
    params.getOrElse(HostsKey, "localhost")
  }

  def getParallelism(params: Map[String, String]): Int = {
    Try(params.getOrElse(LevelParallelism, DefaultLevelParallelism.toString).toInt)
      .getOrElse(DefaultLevelParallelism)
  }

  def getMaxMessagesPerPartition(params: Map[String, String]): Int = {
    Try(params.getOrElse(MaxMessagesPerPartition, DefaultMaxMessagesPerPartition.toString).toInt)
      .getOrElse(DefaultMaxMessagesPerPartition)
  }

  def getQueueConnectionParams(params: Map[String, String]): QueueConnectionOpts = {
    val durable = Try(params.getOrElse(DurableKey, DefaultDurable.toString).toBoolean)
      .getOrElse(DefaultDurable)
    val exclusive = Try(params.getOrElse(ExclusiveKey, DefaultExclusive.toString).toBoolean)
      .getOrElse(DefaultExclusive)
    val autoDelete = Try(params.getOrElse(AutoDeleteKey, DefaultAutoDelete.toString).toBoolean)
      .getOrElse(DefaultAutoDelete)

    QueueConnectionOpts(durable, exclusive, autoDelete)
  }

  def getAddresses(params: Map[String, String]): Array[Address] = {
    val hosts = getHosts(params)

    Address.parseAddresses(hosts)
  }
}
