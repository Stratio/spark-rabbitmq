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
package org.apache.spark.streaming.rabbitmq

object ConfigParameters {

  /**
   * Connection Topology Keys
   */
  val QueueNameKey = "queueName"
  val RoutingKeysKey = "routingKeys"
  val ExchangeNameKey = "exchangeName"
  val ExchangeTypeKey = "exchangeType"
  val ConnectionTopologyKeys = List(QueueNameKey, RoutingKeysKey, ExchangeNameKey, ExchangeTypeKey)

  /**
   * Connection Keys
   */
  val HostsKey = "hosts"
  val VirtualHostKey = "virtualHost"
  val UserNameKey = "userName"
  val PasswordKey = "password"
  val ConnectionKeys = List(HostsKey, VirtualHostKey, UserNameKey, PasswordKey)

  /**
   * Queue Connection properties
   */
  val DurableKey = "durable"
  val ExclusiveKey = "exclusive"
  val AutoDeleteKey = "autoDelete"
  val AckTypeKey = "ackType"
  val QueueConnectionPropertiesKeys = List(DurableKey, ExclusiveKey, AutoDeleteKey, AckTypeKey)

  /**
   * Queue Connection Defaults
   */

  val DefaultDurable = true
  val DefaultExclusive = false
  val DefaultAutoDelete = false
  val DefaultAckType = "auto" //auto, basic, none
  val BasicAckType = "basic"
  val DefaultHost = "localhost"

  /**
   * Spark Consumer properties
   */
  val MaxMessagesPerPartition = "maxMessagesPerPartition"
  val LevelParallelism = "levelParallelism"
  val MaxReceiveTime = "maxReceiveTime"
  val RememberDuration = "rememberDuration"
  val StorageLevelKey = "storageLevel"
  val SparkConsumerPropertiesKeys =
    List(MaxMessagesPerPartition, LevelParallelism, MaxReceiveTime, StorageLevelKey, RememberDuration)

  /**
   * Configuration Defaults
   */
  val DefaultLevelParallelism = 1
  val DefaultMaxMessagesPerPartition = 1000
  val DefaultStorageLevel = "MEMORY_ONLY"
  val DefaultMaxReceiveTime = 0L //when setting 0 is the same as the streaming window
}
