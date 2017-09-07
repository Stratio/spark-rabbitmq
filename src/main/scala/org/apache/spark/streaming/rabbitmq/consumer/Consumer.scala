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

import java.util.concurrent.ConcurrentHashMap

import com.rabbitmq.client.QueueingConsumer.Delivery
import com.rabbitmq.client.{Connection, ConnectionFactory, _}
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.rabbitmq.ConfigParameters._
import org.apache.spark.streaming.rabbitmq.models.{ExchangeAndRouting, QueueConnectionOpts}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
 * With this class you can obtain one queue to consume messages from RabbitMQ
 *
 * @param channel The RabbitMQ channel created for consume messages
 * @param params The parameters that should contains the queue options and the consume options
 */
private[rabbitmq]
class Consumer(val channel: Channel, params: Map[String, String]) extends Logging {

  private var queueName: String = ""

  /**
   * Start one consumer in base of the params of the class
   *
   * @return The queue consumer that can be used to consume messages by the caller
   */
  def startConsumer: QueueingConsumer = {

    assert(queueName.nonEmpty && queueName != "", "The queueName is empty")

    val autoAck = Consumer.getAutoAckFromParams(params)
    val queueConsumer = new QueueingConsumer(channel)

    log.debug(s"Starting consuming data from queue: $queueName")
    channel.basicConsume(queueName, autoAck, queueConsumer)

    queueConsumer
  }

  /**
   * Send one basic ack, this ack correspond with the delivery param
   * @param delivery The previous delivery that the queueConsumer send with one message consumed
   */
  def sendBasicAck(delivery: Delivery): Unit =
    channel.basicAck(delivery.getEnvelope.getDeliveryTag,false)

  /**
   * Send one basic noack, this ack correspond with the delivery param
   * @param delivery The previous delivery that the queueConsumer send with one message consumed
   */
  def sendBasicNAck(delivery: Delivery): Unit =
    channel.basicNack(delivery.getEnvelope.getDeliveryTag,false,true)

  /**
   * Set the number of messages to one when the FairDispatch is called, this is necessary when we want more than one
   * consumer in the same queue
   */
  def setFairDispatchQoS(prefetchCount: Int): Unit = channel.basicQos(prefetchCount)

  /**
   * Functions to call the private method declare queue, this make one connection to one queue in base of all the
   * params: queue name, exchange name and routing key
   */

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

    declareQueue(queueName, exchangeAndRouting, queueConnectionOpts, Consumer.getMessageConsumerParams(usrParams))
  }

  def setQueue(queueName: String, exchangeAndRouting: ExchangeAndRouting, usrParams: Map[String, String]): Unit = {
    val queueConnectionOpts = Consumer.getQueueConnectionParams(params)

    declareQueue(queueName, exchangeAndRouting, queueConnectionOpts, Consumer.getMessageConsumerParams(usrParams))
  }

  def setQueue(queueName: String, usrParams: Map[String, String]): Unit = {
    val exchangeAndRouting = Consumer.getExchangeAndRoutingParams(params)
    val queueConnectionOpts = Consumer.getQueueConnectionParams(params)

    declareQueue(queueName, exchangeAndRouting, queueConnectionOpts, Consumer.getMessageConsumerParams(usrParams))
  }

  def setQueue(queueName: String): Unit =
    declareQueue(queueName)

  def setQueue(queueName: String, exchangeName: String, exchangeType: String): Unit =
    declareQueue(queueName, ExchangeAndRouting(Option(exchangeName), Option(exchangeType), None))

  def setQueue(queueName: String, exchangeName: String, exchangeType: String, routingKeys: String): Unit =
    declareQueue(queueName, ExchangeAndRouting(Option(exchangeName), Option(exchangeType), Option(routingKeys)))

  def close(): Unit = {
    if(channel.isOpen)
      channel.close()
  }

  private def declareQueue(
                            queue: String,
                            exchangeAndRouting: ExchangeAndRouting = ExchangeAndRouting(),
                            queueConnectionOpts: QueueConnectionOpts = QueueConnectionOpts(),
                            queueParams: Map[String, AnyRef] = Map.empty[String, AnyRef]
                          ): Unit = {

    log.debug(s"Declaring Queue: $queue")

    channel.queueDeclare(
      queue,
      queueConnectionOpts.durable,
      queueConnectionOpts.exclusive,
      queueConnectionOpts.autoDelete,
      queueParams
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

  /**
   * Is recommended to use the same factory and the same connection in multithreading
   */
  private val factory = new ConnectionFactory
  private val connections: scala.collection.concurrent.Map[String, Connection] =
    new ConcurrentHashMap[String, Connection]()


  /**
   * Methods to create one Consumer, without user parameters, with parameters and with channel and parameters
   */

  def apply: Consumer = {
    getChannel(Map.empty[String, String]) match {
      case Success(channel) =>
        new Consumer(channel, Map.empty[String, String])
      case Failure(e) =>
        throw new SparkException(s"Error creating channel and connection: ${e.getLocalizedMessage}")
    }
  }

  def apply(channel: Channel, params: Map[String, String]): Consumer =
    new Consumer(channel, params)

  def apply(params: Map[String, String]): Consumer = {

    setVirtualHost(params)
    setUserPassword(params)

    getChannel(params) match {
      case Success(channel) =>
        new Consumer(channel, params)
      case Failure(e) =>
        throw new SparkException(s"Error creating channel and connection: ${e.getLocalizedMessage}")
    }
  }

  /**
   * Close all connections in this singleton
   */
  def closeConnections(): Unit =
    connections.foreach{case (key, connection) =>
      try {
        if (connection.isOpen)
          connection.close(DefaultCodeClose.toInt, s"Closing connection with key: $key")
      } finally {
        connections.remove(key)
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

  private def getChannel(params: Map[String, String]): Try[Channel] = {
    val addresses = getAddresses(params)
    val addressesKey = addresses.mkString(",")
    val connection = {
      if(connections.contains(addressesKey))
        connections(addressesKey)
      else addConnection(addressesKey, addresses)
    }

    log.debug("Creating new channel")

    val channel = Try(connection.createChannel)
    channel match {
      case Failure(e) =>
        try {
          if (connection.isOpen) {
            connection.close(
              params.getOrElse(CodeClose, DefaultCodeClose).toInt,
              "Closing connection as we can't create a channel with it ..."
            )
          }
        } finally {
          log.warn(s"Failed to createChannel ${e.getMessage}. Remove connection $addressesKey")
          connections.remove(addressesKey)
        }
      case _ =>
        log.debug("Channel created correctly")
    }
    channel
  }

  private def addConnection(key: String, addresses: Array[Address]) : Connection = {
    val conn = factory.newConnection(addresses)
    connections.put(key, conn)
    conn
  }
}
