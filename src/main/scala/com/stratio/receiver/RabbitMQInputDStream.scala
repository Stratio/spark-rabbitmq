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
import org.joda.time.DateTime

import scala.util._

import com.rabbitmq.client._
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.JavaConverters._

private[receiver]
class RabbitMQInputDStream(@transient ssc_ : StreamingContext,
                            params: Map[String, String]
                            ) extends ReceiverInputDStream[String](ssc_) with Logging {

  private val storageLevelParam: String = params.getOrElse("storageLevel", "MEMORY_AND_DISK_SER_2")

  override def getReceiver(): Receiver[String] = {

    new RabbitMQReceiver(params, StorageLevel.fromString(storageLevelParam))
  }
}

private[receiver]
class RabbitMQReceiver(params: Map[String, String], storageLevel: StorageLevel)
  extends Receiver[String](storageLevel) with Logging {

  private val host: String = params.getOrElse("host", "localhost")
  private val rabbitMQQueueName: Option[String] = params.get("queueName")
  private val exchangeName: String = params.getOrElse("exchangeName", "rabbitmq-exchange")
  private val exchangeType: String = params.getOrElse("exchangeType", "direct")
  private val routingKeys: Option[String] = params.get("routingKeys")
  private val vHost: Option[String] = params.get("vHost")
  private val username: Option[String] = params.get("username")
  private val password: Option[String] = params.get("password")
  private val x_max_lenght: Option[String] = params.get("x-max-length")
  private val x_message_ttl: Option[String] = params.get("x-message-ttl")
  private val x_expires: Option[String] = params.get("x-expires")
  private val x_max_length_bytes: Option[String] = params.get("x-max-length-bytes")
  private val x_dead_letter_exchange: Option[String] = params.get("x-dead-letter-exchange")
  private val x_dead_letter_routing_key: Option[String] = params.get("x-dead-letter-routing-key")
  private val x_max_priority: Option[String] = params.get("x-max-priority")

  val DirectExchangeType: String = "direct"
  val TopicExchangeType: String = "topic"
  val DefaultRabbitMQPort = 5672

      
  def onStart() {
    implicit val akkaSystem = akka.actor.ActorSystem()
    getConnectionAndChannel match {
      case Success((connection: Connection, channel: Channel)) => log.info("onStart, Connecting..")
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
    // Get the queue name to use (explicit vs auto-generated).
    val queueName = checkQueueName()

    // Connect to the exchange.
    log.info(s"declaring exchange '$exchangeName' of type '$exchangeType'")
    channel.exchangeDeclare(exchangeName, exchangeType, true)

    log.info("declaring queue")
    channel.queueDeclare(queueName, true, false, false, getParams.asJava)

    // Bind the exchange to the queue.
    routingKeys match {
      case Some(routingKey) => {
        // If routing keys were provided, then bind using them.
        for (routingKey: String <- routingKey.split(",")) {
          log.info(s"binding to routing key '$routingKey'")
          channel.queueBind(queueName, exchangeName, routingKey)
        }
      }
      case None => {
        log.info("binding exchange using empty routing key")
        channel.queueBind(queueName, exchangeName, "")
      }
    }
    queueName
  }

   def getParams() : Map[String, AnyRef] = {
     var params: Map[String, AnyRef] = Map.empty

     if (x_max_lenght.isDefined) {
       params += ("x-max-length" -> x_max_lenght.get.toInt.asInstanceOf[AnyRef])
     }
     if (x_message_ttl.isDefined) {
       params += ("x-message-ttl" -> x_message_ttl.get.toInt.asInstanceOf[AnyRef])
     }
     if (x_expires.isDefined) {
       params += ("x-expires" -> x_expires.get.toInt.asInstanceOf[AnyRef])
     }
     if (x_max_length_bytes.isDefined) {
       params += ("x-max-length-bytes" -> x_max_length_bytes.get.toInt.asInstanceOf[AnyRef])
     }
     if (x_dead_letter_exchange.isDefined) {
       params += ("x-dead-letter-exchange" -> x_dead_letter_exchange.get.toString.asInstanceOf[AnyRef])
     }
     if (x_dead_letter_routing_key.isDefined) {
       params += ("x-dead-letter-routing-key" -> x_dead_letter_routing_key.get.toString.asInstanceOf[AnyRef])
     }
     if (x_max_priority.isDefined) {
       params += ("x-max-priority" -> x_max_priority.get.toInt.asInstanceOf[AnyRef])
     }
     params
   }


  def checkQueueName(): String = {
    rabbitMQQueueName.getOrElse({
      log.warn("The name of the queue will be a default name")
      s"default-queue-${new DateTime(System.currentTimeMillis())}"
    })
  }

  private def getConnectionAndChannel: Try[(Connection, Channel)] = {
    log.info("Rabbit host addresses are : " + host)
    for ( address <- Address.parseAddresses(host) ) {
      log.info("Address " + address.toString())
    }

    log.info("creating new connection and channel")
    for {
      connection: Connection <- Try(getConnectionFactory.newConnection(Address.parseAddresses(host)))
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
