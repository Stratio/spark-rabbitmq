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

package org.apache.spark.streaming.rabbitmq

import java.util

import com.rabbitmq.client.{Connection, ConnectionFactory, _}
import ConfigParameters._
import org.apache.spark.streaming.rabbitmq.distributed.RabbitMQDistributedKey
import org.apache.spark.{Logging, SparkException}

import scala.util.{Failure, Success, Try}

private[rabbitmq]
class Consumer(val connection: Connection, val channel: Channel, params: Map[String, String]) extends Logging {

  private var queueName: String = ""

  def startConsumer: QueueingConsumer = {
    val autoAck = Try(params.getOrElse(AutoAckKey, "true").toBoolean).getOrElse(true)
    val queueConsumer = new QueueingConsumer(channel)

    log.info(s"Starting consuming data from queue: $queueName")
    channel.basicConsume(queueName, autoAck, queueConsumer)

    queueConsumer
  }

  def setFairDispatchQoS(): Unit = channel.basicQos(1)

  def setQueue(): Unit = setQueue(params)

  def setQueue(params: Map[String, String]): Unit = {
    val queueName = params.get(QueueNameKey)

    queueName match {
      case Some(queue) => setQueue(queue, params)
      case None => throw new Exception(s"Is mandatory to specify one 'queueName' in options")
    }
  }

  def setQueue(
                queueName: String,
                exchangeName: Option[String],
                exchangeType: Option[String],
                routingKeys: Option[String],
                usrParams: Map[String, String]
              ): Unit = {
    val durable = Try(params.getOrElse(DurableKey, "true").toBoolean).getOrElse(true)
    val exclusive = Try(params.getOrElse(ExclusiveKey, "false").toBoolean).getOrElse(false)
    val autoDelete = Try(params.getOrElse(AutoDeleteKey, "false").toBoolean).getOrElse(false)

    declareQueue(queueName, exchangeName, exchangeType, routingKeys, durable, exclusive, autoDelete)
  }

  def setQueue(queueName: String, usrParams: Map[String, String]): Unit = {
    val routingKeys = params.get(RoutingKeysKey)
    val exchangeName = params.get(ExchangeNameKey)
    val exchangeType = params.get(ExchangeTypeKey)
    val durable = Try(params.getOrElse(DurableKey, "true").toBoolean).getOrElse(true)
    val exclusive = Try(params.getOrElse(ExclusiveKey, "false").toBoolean).getOrElse(false)
    val autoDelete = Try(params.getOrElse(AutoDeleteKey, "false").toBoolean).getOrElse(false)

    declareQueue(queueName, exchangeName, exchangeType, routingKeys, durable, exclusive, autoDelete)
  }

  def setQueue(queueName: String): Unit =
    declareQueue(queueName)

  def setQueue(queueName: String, exchangeName: String, exchangeType: String): Unit =
    declareQueue(queueName, Option(exchangeName), Option(exchangeType), None)

  def setQueue(queueName: String, exchangeName: String, exchangeType: String, routingKeys: String): Unit =
    declareQueue(queueName, Option(exchangeName), Option(exchangeType), Option(routingKeys))

  def close(): Unit = {
    channel.close()
    connection.close()
  }

  private def declareQueue(
                            queue: String,
                            exchangeName: Option[String] = None,
                            exchangeType: Option[String] = None,
                            routingKeys: Option[String] = None,
                            durable: Boolean = true,
                            exclusive: Boolean = false,
                            autoDelete: Boolean = false
                          ): Unit = {

    log.info(s"Declaring Queue: $queue")
    channel.queueDeclare(queue, durable, exclusive, autoDelete, new util.HashMap(0))

    if (exchangeName.isDefined && exchangeType.isDefined) {
      log.info(s"Declaring Exchange: ${exchangeName.get} with type: ${exchangeType.get.toString}")
      channel.exchangeDeclare(exchangeName.get, exchangeType.get, durable)
      routingKeys.foreach(routingKey =>
        routingKey.split(",").foreach(key => {
          log.info("Binding to routing key " + key)
          channel.queueBind(queue, exchangeName.get, key)
        })
      )
    }
    queueName = queue
  }
}

private[rabbitmq]
object Consumer extends Logging {

  private val factory: ConnectionFactory = new ConnectionFactory

  def apply: Consumer = {
    getConnectionAndChannel(Map.empty[String, String]) match {
      case Success((connection, channel)) =>
        new Consumer(connection, channel, Map.empty[String, String])
      case Failure(e) =>
        throw new SparkException(s"Error creating channel and connection: ${e.getLocalizedMessage}")
    }
  }

  def apply(connection: Connection, channel: Channel, params: Map[String, String]): Consumer =
    new Consumer(connection, channel, params)

  def apply(params: Map[String, String]): Consumer = {

    setVirtualHost(params)
    setUserPassword(params)

    getConnectionAndChannel(params) match {
      case Success((connection, channel)) =>
        new Consumer(connection, channel, params)
      case Failure(e) =>
        throw new SparkException(s"Error creating channel and connection: ${e.getLocalizedMessage}")
    }
  }

  private[rabbitmq] def getExchangesDistributed(params: Map[String, String]): Seq[RabbitMQDistributedKey] = {
    val queueName = params.get(QueueNameKey)

    queueName.fold(Seq.empty[RabbitMQDistributedKey]) { queue => {
      val routingKeys = params.get(RoutingKeysKey)
      val exchangeName = params.get(ExchangeNameKey)
      val exchangeType = params.get(ExchangeTypeKey)

      Seq(RabbitMQDistributedKey(queue, exchangeName, exchangeType, routingKeys))
    }
    }
  }

  private[rabbitmq] def getHosts(params: Map[String, String]): String = {
    params.getOrElse(HostsKey, "localhost")
  }

  private[rabbitmq] def getMaxMessagesPerPartition(params: Map[String, String]): Int = {
    Try(params.getOrElse(MaxMessagesPerPartition, "1000").toInt).getOrElse(1000)
  }

  private def getAddresses(params: Map[String, String]): Array[Address] = {
    val hosts = getHosts(params)

    Address.parseAddresses(hosts)
  }

  private def getConnectionAndChannel(params: Map[String, String]): Try[(Connection, Channel)] = {
    val addresses = getAddresses(params)

    log.info("Address " + addresses.mkString(","))
    log.info("Creating new connection and channel")

    for {
      connection <- Try(factory.newConnection(addresses))
      channel <- Try(connection.createChannel)
    } yield {
      log.info("Created new connection and channel")

      (connection, channel)
    }
  }

  private def setVirtualHost(params: Map[String, String]): Unit = {
    val vHost = params.get(VirtualHostKey)

    vHost.foreach(virtualHost => {
      factory.setVirtualHost(virtualHost)
      log.info(s"Connecting to virtual host ${factory.getVirtualHost}")
    })
  }

  private def setUserPassword(params: Map[String, String]): Unit = {
    val username = params.get(UserNameKey)
    val password = params.get(PasswordKey)

    username.foreach(user => {
      factory.setUsername(user)
      log.info(s"Setting userName ${factory.getUsername}")
    })
    password.foreach(pwd => {
      factory.setPassword(pwd)
      log.info(s"Setting password ${factory.getPassword}")
    })
  }
}
