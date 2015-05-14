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

import java.net.ConnectException
import java.util

import scala.util.{Failure, Try}

import akka.actor.Status.Success
import com.rabbitmq.client.{Channel, Connection, ConnectionFactory, QueueingConsumer}
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver


private[receiver]
class RabbitMQInputDStream(
                         @transient ssc_ : StreamingContext,
                         rabbitMQQueueName: Option[String],
                         rabbitMQHost: String,
                         rabbitMQPort: Int,
                         exchangeName: Option[String],
                         routingKeys: Seq[String],
                         storageLevel: StorageLevel
                         ) extends ReceiverInputDStream[String](ssc_) with Logging {

  override def getReceiver(): Receiver[String] = {
    val DefaultRabbitMQPort = 5672

    new RabbitMQReceiver(rabbitMQQueueName,
      Some(rabbitMQHost).getOrElse("localhost"),
      Some(rabbitMQPort).getOrElse(DefaultRabbitMQPort),
      exchangeName,
      routingKeys,
      storageLevel)
  }
}

private[receiver]
class RabbitMQReceiver(rabbitMQQueueName: Option[String],
                       rabbitMQHost: String,
                       rabbitMQPort: Int,
                       exchangeName: Option[String],
                       routingKeys: Seq[String],
                       storageLevel: StorageLevel)
  extends Receiver[String](storageLevel) with Logging {

  val DirectExchangeType: String = "direct"

  def onStart() {
    implicit val akkaSystem = akka.actor.ActorSystem()
    receive()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {

      val (connection: Connection, channel: Channel) = for {
        factory: ConnectionFactory <- getConnectionFactory
        connection: Connection <- getNewConnection(factory)
        channel <- connection.createChannel
      } yield {
        (connection, channel)
      }

      var queueName = ""
      if (!routingKeys.isEmpty){
        channel.exchangeDeclare(exchangeName.get, DirectExchangeType)
        queueName = channel.queueDeclare().getQueue()

        for (routingKey: String <- routingKeys) {
          channel.queueBind(queueName, exchangeName.get, routingKey)
        }
      }else{
        channel.queueDeclare(rabbitMQQueueName.get, false, false, false, new util.HashMap(0))
        queueName = rabbitMQQueueName.get
      }

      log.error("RabbitMQ Input waiting for messages")
      val consumer: QueueingConsumer = new QueueingConsumer(channel)
      channel.basicConsume(queueName, true, consumer)
      while (!isStopped) {
        val delivery: QueueingConsumer.Delivery = consumer.nextDelivery
        store(new String(delivery.getBody))
      }

      log.info("it has been stopped")
      channel.close
      connection.close
      restart("Trying to connect again")
  }

  private val getConnectionFactory: ConnectionFactory = {
    val factory: ConnectionFactory = new ConnectionFactory
    factory.setHost(rabbitMQHost)
    factory.setPort(rabbitMQPort)
    factory
  }

  private def getNewConnection(factory: ConnectionFactory): Connection = {
    Try(factory.newConnection()) match {
      case Success(connection: Connection) => connection
      case Failure(f) =>   log.error("Could not connect"); restart("Could not connect", f); throw f
    }
  }

}
