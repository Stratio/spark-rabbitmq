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
package org.apache.spark.streaming.rabbitmq.consumer

import com.rabbitmq.client.Address
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.rabbitmq.ConfigParameters._
import org.apache.spark.streaming.rabbitmq.distributed.RabbitMQDistributedKey
import org.apache.spark.streaming.rabbitmq.models.{ExchangeAndRouting, QueueConnectionOpts}

import scala.util.Try

private[rabbitmq]
trait ConsumerParamsUtils {

  /**
   * Grouped params by functionality
   */
  def getConnectionParams(params: Map[String, String]): Map[String, String] =
    params filterKeys ConnectionKeys.contains

  def getConnectionTopologyParams(params: Map[String, String]): Map[String, String] =
    params filterKeys ConnectionTopologyKeys.contains

  def getQueueConnectionPropertiesParams(params: Map[String, String]): Map[String, String] =
    params filterKeys QueueConnectionPropertiesKeys.contains

  def getSparkConsumerPropertiesParams(params: Map[String, String]): Map[String, String] =
    params filterKeys SparkConsumerPropertiesKeys.contains

  /**
   * Connection params
   */
  def getHosts(params: Map[String, String]): String =
    params.getOrElse(HostsKey, DefaultHost)

  def getAddresses(params: Map[String, String]): Array[Address] = {
    val hosts = getHosts(params)

    Address.parseAddresses(hosts)
  }

  def getDistributedKeysParams(params: Map[String, String]): Seq[RabbitMQDistributedKey] = {
    val queueName = params.get(QueueNameKey)
    val connectionParams = getConnectionParams(params)

    queueName.fold(Seq.empty[RabbitMQDistributedKey]) { queue => {
      val exchangeAndRouting = getExchangeAndRoutingParams(params)

      Seq(RabbitMQDistributedKey(queue, exchangeAndRouting, connectionParams))
    }
    }
  }

  def getExchangeAndRoutingParams(params: Map[String, String]): ExchangeAndRouting = {
    val routingKeys = params.get(RoutingKeysKey)
    val exchangeName = params.get(ExchangeNameKey)
    val exchangeType = params.get(ExchangeTypeKey)

    ExchangeAndRouting(exchangeName, exchangeType, routingKeys)
  }

  /**
   * Queue Properties
   */
  def getQueueConnectionParams(params: Map[String, String]): QueueConnectionOpts = {
    val durable = Try(params.getOrElse(DurableKey, DefaultDurable.toString).toBoolean)
      .getOrElse(DefaultDurable)
    val exclusive = Try(params.getOrElse(ExclusiveKey, DefaultExclusive.toString).toBoolean)
      .getOrElse(DefaultExclusive)
    val autoDelete = Try(params.getOrElse(AutoDeleteKey, DefaultAutoDelete.toString).toBoolean)
      .getOrElse(DefaultAutoDelete)

    QueueConnectionOpts(durable, exclusive, autoDelete)
  }

  def getAutoAckFromParams(params: Map[String, String]): Boolean =
    params.getOrElse(AckTypeKey, DefaultAckType) match {
      case AutoAckType => true
      case _ => false
    }

  def getFairDispatchFromParams(params: Map[String, String]): Boolean =
    Try(params.getOrElse(FairDispatchKey, DefaultFairDispatch.toString).toBoolean)
      .getOrElse(DefaultFairDispatch)

  def getPrefetchCountFromParams(params: Map[String, String]): Int =
    Try(params.getOrElse(PrefetchCount, DefaultPrefetchCount.toString).toInt)
      .getOrElse(DefaultPrefetchCount)

  def getAckFromParams(params: Map[String, String]): String =
    params.getOrElse(AckTypeKey, DefaultAckType)

  def sendingBasicAckFromParams(params: Map[String, String]): Boolean =
    getAckFromParams(params) == BasicAckType

  /**
   * Spark properties
   */
  def getMaxReceiveTime(params: Map[String, String]): Long =
    Try(params.getOrElse(MaxReceiveTime, DefaultMaxReceiveTime.toString).toLong)
      .getOrElse(DefaultMaxReceiveTime)

  def getParallelism(params: Map[String, String]): Int =
    Try(params.getOrElse(LevelParallelism, DefaultLevelParallelism.toString).toInt)
      .getOrElse(DefaultLevelParallelism)

  def getRememberDuration(params: Map[String, String]): Option[Long] =
    Try(params.get(RememberDuration).map(_.toLong)).getOrElse(None)

  def getStorageLevel(params: Map[String, String]): StorageLevel =
    StorageLevel.fromString(Try(params.getOrElse(StorageLevelKey, DefaultStorageLevel))
      .getOrElse(DefaultStorageLevel))

  def getMaxMessagesPerPartition(params: Map[String, String]): Option[Int] =
    params.get(MaxMessagesPerPartition).map(_.toInt)

  /**
   * Consumer Messages
   */
  def getMessageConsumerParams(params: Map[String, String]): Map[String, AnyRef] = {
    def paramValue(key: String, param: String): AnyRef = {
      key match {
        case XmaxLength | XmessageTtl | Xexpires | XmaxLengthBytes | XmaxPriority => param.toInt.asInstanceOf[AnyRef]
        case _ => param
      }
    }
    for {
      key <- MessageConsumerPropertiesKeys
      param <- params.get(key)
    } yield (key, paramValue(key, param))
  }.toMap
}
