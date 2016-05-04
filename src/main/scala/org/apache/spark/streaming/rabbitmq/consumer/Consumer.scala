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

import java.util

import com.rabbitmq.client.QueueingConsumer.Delivery
import com.rabbitmq.client.{Connection, ConnectionFactory, _}
import org.apache.spark.streaming.rabbitmq.ConfigParameters._
import org.apache.spark.streaming.rabbitmq.models.{ExchangeAndRouting, QueueConnectionOpts}
import org.apache.spark.{Logging, SparkException}

import scala.util.{Failure, Success, Try}

private[rabbitmq]
class Consumer(val connection: Connection, val channel: Channel, params: Map[String, String]) extends Logging {

  private var queueName: String = ""

  def startConsumer: QueueingConsumer = {

    assert(queueName.nonEmpty && queueName != "", "The queueName is empty")

    val autoAck = Consumer.getAutoAckFromParams(params)
    val queueConsumer = new QueueingConsumer(channel)

    log.debug(s"Starting consuming data from queue: $queueName")
    channel.basicConsume(queueName, autoAck, queueConsumer)

    queueConsumer
  }

  def sendBasicAck(delivery: Delivery): Unit =
    channel.basicAck(delivery.getEnvelope.getDeliveryTag,false)

  def setFairDispatchQoS(): Unit = channel.basicQos(1)

  def setQueue(): Unit = setQueue(params)

  def setQueue(params: Map[String, String]): Unit = {
    val queueName = params.get(QueueNameKey)

    queueName match {
      case Some(queue) => setQueue(queue, params)
      case None => throw new SparkException(s"Is mandatory to specify one 'queueName' in options")
    }
  }

  def setQueue(
                queueName: String,
                exchangeName: Option[String],
                exchangeType: Option[String],
                routingKeys: Option[String],
                usrParams: Map[String, String]
              ): Unit = {
    val exchangeAndRouting = ExchangeAndRouting(exchangeName, exchangeType, routingKeys)
    val queueConnectionOpts = Consumer.getQueueConnectionParams(params)

    declareQueue(queueName, exchangeAndRouting, queueConnectionOpts)
  }

  def setQueue(queueName: String, exchangeAndRouting: ExchangeAndRouting, usrParams: Map[String, String]): Unit = {
    val queueConnectionOpts = Consumer.getQueueConnectionParams(params)

    declareQueue(queueName, exchangeAndRouting, queueConnectionOpts)
  }

  def setQueue(queueName: String, usrParams: Map[String, String]): Unit = {
    val exchangeAndRouting = Consumer.getExchangeAndRoutingParams(params)
    val queueConnectionOpts = Consumer.getQueueConnectionParams(params)

    declareQueue(queueName, exchangeAndRouting, queueConnectionOpts)
  }

  def setQueue(queueName: String): Unit =
    declareQueue(queueName)

  def setQueue(queueName: String, exchangeName: String, exchangeType: String): Unit =
    declareQueue(queueName, ExchangeAndRouting(Option(exchangeName), Option(exchangeType), None))

  def setQueue(queueName: String, exchangeName: String, exchangeType: String, routingKeys: String): Unit =
    declareQueue(queueName, ExchangeAndRouting(Option(exchangeName), Option(exchangeType), Option(routingKeys)))

  def close(): Unit = {
    channel.close()
    connection.close()
  }

  private def declareQueue(
                            queue: String,
                            exchangeAndRouting: ExchangeAndRouting = ExchangeAndRouting(),
                            queueConnectionOpts: QueueConnectionOpts = QueueConnectionOpts()
                          ): Unit = {

    log.debug(s"Declaring Queue: $queue")
    channel.queueDeclare(
      queue,
      queueConnectionOpts.durable,
      queueConnectionOpts.exclusive,
      queueConnectionOpts.autoDelete,
      new util.HashMap(0)
    )

    if (exchangeAndRouting.exchangeName.isDefined && exchangeAndRouting.exchangeType.isDefined) {
      log.debug(s"Declaring Exchange: ${exchangeAndRouting.exchangeName.get} with type:" +
        s" ${exchangeAndRouting.exchangeType.get.toString}")

      channel.exchangeDeclare(
        exchangeAndRouting.exchangeName.get,
        exchangeAndRouting.exchangeType.get,
        queueConnectionOpts.durable
      )

      exchangeAndRouting.routingKeys.foreach(routingKey =>
        routingKey.split(",").foreach(key => {
          log.debug("Binding to routing key " + key)
          channel.queueBind(queue, exchangeAndRouting.exchangeName.get, key)
        })
      )
    }

    queueName = queue
  }
}

private[rabbitmq]
object Consumer extends Logging with ConsumerParamsUtils {

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

  private def setVirtualHost(params: Map[String, String]): Unit = {
    val vHost = params.get(VirtualHostKey)

    vHost.foreach(virtualHost => {
      factory.setVirtualHost(virtualHost)
      log.debug(s"Connecting to virtual host ${factory.getVirtualHost}")
    })
  }

  private def setUserPassword(params: Map[String, String]): Unit = {
    val username = params.get(UserNameKey)
    val password = params.get(PasswordKey)

    username.foreach(user => {
      factory.setUsername(user)
      log.debug(s"Setting userName ${factory.getUsername}")
    })
    password.foreach(pwd => {
      factory.setPassword(pwd)
      log.debug(s"Setting password ${factory.getPassword}")
    })
  }

  private def getConnectionAndChannel(params: Map[String, String]): Try[(Connection, Channel)] = {
    val addresses = getAddresses(params)

    log.debug("Address " + addresses.mkString(","))
    log.debug("Creating new connection and channel")

    for {
      connection <- Try(factory.newConnection(addresses))
      channel <- Try(connection.createChannel)
    } yield {
      log.debug("Created new connection and channel")

      (connection, channel)
    }
  }
}
