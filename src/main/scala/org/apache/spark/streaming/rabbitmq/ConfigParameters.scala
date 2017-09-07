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
  val CodeClose = "codeClose"
  val ConnectionKeys = List(HostsKey, VirtualHostKey, UserNameKey, PasswordKey, CodeClose)

  /**
   * Queue Connection properties
   */
  val DurableKey = "durable"
  val ExclusiveKey = "exclusive"
  val AutoDeleteKey = "autoDelete"
  val AckTypeKey = "ackType"
  val FairDispatchKey = "fairDispatch"
  val PrefetchCount = "prefetchCount"
  val QueueConnectionPropertiesKeys =
    List(DurableKey, ExclusiveKey, AutoDeleteKey, AckTypeKey, FairDispatchKey, PrefetchCount)

  /**
   * Queue Connection Defaults
   */
  val DefaultDurable = true
  val DefaultExclusive = false
  val DefaultAutoDelete = false
  val DefaultFairDispatch = false
  val DefaultAckType = "basic"
  val BasicAckType = "basic"
  val AutoAckType = "auto"
  val DefaultHost = "localhost"
  val DefaultPrefetchCount = 1
  val DefaultCodeClose = "320"

  /**
   * Message Consumed properties
   */
  val XmaxLength = "x-max-length"
  val XmessageTtl = "x-message-ttl"
  val Xexpires = "x-expires"
  val XmaxLengthBytes = "x-max-length-bytes"
  val XDeadLetterExchange = "x-dead-letter-exchange"
  val XdeadLetterRoutingKey = "x-dead-letter-routing-key"
  val XmaxPriority = "x-max-priority"
  val MessageConsumerPropertiesKeys =
    List(XmaxLength, XmessageTtl, Xexpires, XmaxLengthBytes, XDeadLetterExchange, XdeadLetterRoutingKey, XmaxPriority)

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
  val DefaultRateReceiveCompute = 0.9
  val DefaultMinRememberDuration = "60s"
}
