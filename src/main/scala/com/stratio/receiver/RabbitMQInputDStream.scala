/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
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

package com.stratio.receiver

import java.util

import com.rabbitmq.client.QueueingConsumer.Delivery

import scala.util._

import com.rabbitmq.client._
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

private[receiver]
class RabbitMQInputDStream(@transient ssc_ : StreamingContext,
                            params: Map[String, String]
                            ) extends ReceiverInputDStream[String](ssc_) with Logging {

  private val storageLevelParam: String = params.get("storageLevel").getOrElse("MEMORY_AND_DISK_SER_2")

  override def getReceiver(): Receiver[String] = {

    new RabbitMQReceiver(params, StorageLevel.fromString(storageLevelParam))
  }
}

private[receiver]
class RabbitMQReceiver(params: Map[String, String], storageLevel: StorageLevel)
  extends Receiver[String](storageLevel) with Logging {

  private val host: Option[String] = params.get("host")
  private val rabbitMQQueueName: Option[String] = params.get("queueName")
  private val exchangeName: Option[String] = params.get("exchangeName")
  private val exchangeType: String = params.get("exchangeType").getOrElse("direct")
  private val routingKeys: Option[String] = params.get("routingKeys")
  private val vHost: Option[String] = params.get("vHost")
  private val username: Option[String] = params.get("username")
  private val password: Option[String] = params.get("password")

  val DirectExchangeType: String = "direct"
  val TopicExchangeType: String = "topic"
  val DefaultRabbitMQPort = 5672

      
  def onStart() {
    implicit val akkaSystem = akka.actor.ActorSystem()
    getConnectionAndChannel match {
      //case Success((connection: Connection, channel: Channel)) => log.info("onStart, Connecting.."); receive(connection, channel)
      case Success((connection: Connection, channel: Channel)) => log.info("onStart, Connecting..");
        new Thread() {
          override def run() {
            receive(connection, channel)
          }
        }.start()
      case Failure(f) => log.error("Could not connect"); restart("Could not connect", f)
    }
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
    log.info("onStop, doing nothing.. relaxing...")
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive(connection: Connection, channel: Channel) {

    try {
      val queueName: String = getQueueName(channel)
  
      log.info("RabbitMQ Input waiting for messages")
      val consumer: QueueingConsumer = new QueueingConsumer(channel)
      log.info("start consuming data")
      channel.basicConsume(queueName, true, consumer)
  
      while (!isStopped()) {
        log.info("waiting for data")
        val delivery: Delivery = consumer.nextDelivery()
        log.info("storing data")
        store(new Predef.String(delivery.getBody))
      }

    } catch {
      case unknown : Throwable => log.error("Got this unknown exception: " + unknown, unknown)
    } 
    finally {
      log.info("it has been stopped")
      try { channel.close } catch { case _: Throwable => log.error("error on close channel, ignoring")}
      try { connection.close} catch { case _: Throwable => log.error("error on close connection, ignoring")}
      restart("Trying to connect again")
    }
  }

  def getQueueName(channel: Channel): String = {
    routingKeys.isDefined match {
      case true => {
        log.info("declaring topic queue")
        channel.exchangeDeclare(exchangeName.get, exchangeType, true)
        val queueName = channel.queueDeclare().getQueue()

        for (routingKey: String <- routingKeys.get.split(",")) {
          log.info("binding to routing key " + routingKey)
          channel.queueBind(queueName, exchangeName.get, routingKey)
        }
        queueName
      }
      case false => {
        log.info("declaring direct queue")
        channel.queueDeclare(rabbitMQQueueName.get, true, false, false, new util.HashMap(0))
        rabbitMQQueueName.get
      }
    }
  }

  private def getConnectionAndChannel: Try[(Connection, Channel)] = {
    log.info("Rabbit host addresses are :" + host.get)
    for ( address <- Address.parseAddresses(host.get) ) {
      log.info("Address " + address.toString())
    }

    log.info("creating new connection and channel")
    for {
      connection: Connection <- Try(getConnectionFactory.newConnection(Address.parseAddresses(host.get)))
      channel: Channel <- Try(connection.createChannel)
    } yield {
      log.info("created new connection and channel")
      (connection, channel)
    }
  }

  private def getConnectionFactory: ConnectionFactory = {
    val factory: ConnectionFactory = new ConnectionFactory

    vHost match {
      case Some(v) => {
        factory.setVirtualHost(v)
        log.info(s"Connecting to virtual host ${factory.getVirtualHost}")
      }
      case None =>
        log.info("No virtual host configured")
    }

    username.map(factory.setUsername(_))
    password.map(factory.setPassword(_))

    factory
  }
}
