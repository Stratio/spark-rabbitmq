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

import akka.actor.ActorSystem
import com.github.sstone.amqp.Amqp._
import com.github.sstone.amqp.{Amqp, ChannelOwner, ConnectionOwner, Consumer}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.ConnectionFactory
import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import akka.testkit.TestProbe
import akka.util.Timeout
import akka.pattern.{ask, gracefulStop}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

private[rabbitmq] trait TemporalDataSuite extends RabbitMQSuite with BeforeAndAfter with BeforeAndAfterAll {

  implicit val system = ActorSystem("ActorRabbitMQSystem")
  implicit val timeout = Timeout(10 seconds)

  private lazy val config = ConfigFactory.load()

  val queueName : String

  val exchangeName : String

  val totalRegisters = 10000

  /**
   * Spark Properties
   */
  val conf = new SparkConf()
    .setAppName("datasource-receiver-example")
    .setIfMissing("spark.master", "local[*]")
  var sc: SparkContext = null
  var ssc: StreamingContext = null

  /**
   * RabbitMQ Properties
   */
  val configQueueName = Try(config.getString("rabbitmq.queueName")).getOrElse("rabbitmq-queue")
  val configExchangeName = Try(config.getString("rabbitmq.exchangeName")).getOrElse("rabbitmq-exchange")
  val exchangeType = Try(config.getString("rabbitmq.exchangeType")).getOrElse("topic")
  val routingKey = Try(config.getString("rabbitmq.routingKey")).getOrElse("")
  val vHost = Try(config.getString("rabbitmq.vHost")).getOrElse("/")
  val hosts = Try(config.getString("rabbitmq.hosts")).getOrElse("127.0.0.1")
  val userName = Try(config.getString("rabbitmq.userName")).getOrElse("guest")
  val password = Try(config.getString("rabbitmq.password")).getOrElse("guest")

  before {

    val probe = TestProbe()
    val queue = QueueParameters(
      name = queueName,
      passive = false,
      exclusive = false,
      durable = true,
      autodelete = false
    )
    val exchange = ExchangeParameters(
      name = exchangeName,
      passive = false,
      exchangeType = exchangeType,
      durable = true,
      autodelete = false
    )
    val connFactory = new ConnectionFactory()
    connFactory.setUri(s"amqp://$userName:$password@$hosts/%2F")
    val conn = system.actorOf(ConnectionOwner.props(connFactory, 1 second))
    Amqp.waitForConnection(system, conn).await()
    val consumer = ConnectionOwner.createChildActor(
      conn,
      Consumer.props(listener = Some(probe.ref)),
      timeout = 5000 millis,
      name = Some("RabbitMQ.consumer")
    )
    val producer = ConnectionOwner.createChildActor(
      conn,
      ChannelOwner.props(),
      timeout = 5000 millis,
      name = Some("RabbitMQ.producer")
    )

    Amqp.waitForConnection(system, conn, consumer, producer).await()

    val deleteQueueResult = consumer ? DeleteQueue(queueName)
    Await.result(deleteQueueResult, 5 seconds)
    val deleteExchangeResult = consumer ? DeleteExchange(exchangeName)
    Await.result(deleteExchangeResult, 5 seconds)
    val bindingResult = consumer ? AddBinding(Binding(exchange, queue, routingKey))
    Await.result(bindingResult, 5 seconds)

    sc = new SparkContext(conf)
    ssc = new StreamingContext(sc, Seconds(1))

    for (register <- 1 to totalRegisters) {
      val publishResult = producer ? Publish(
        exchange = exchange.name,
        key = "",
        body = register.toString.getBytes,
        properties = Some(new BasicProperties.Builder().contentType("my " + "content").build())
      )
      Await.result(publishResult, 1 seconds)
    }

    /**
     * Close Producer actors and connections
     */
    conn ! Close()
    Await.result(gracefulStop(conn, 5 seconds), 10 seconds)
    Await.result(gracefulStop(consumer, 5 seconds), 10 seconds)
    Await.result(gracefulStop(producer, 5 seconds), 10 seconds)
  }

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
    if (sc != null) {
      sc.stop()
      sc = null
    }

    System.gc()

    val probe = TestProbe()
    val connFactory = new ConnectionFactory()
    connFactory.setUri(s"amqp://$userName:$password@$hosts/%2F")
    val conn = system.actorOf(ConnectionOwner.props(connFactory, 1 second))
    Amqp.waitForConnection(system, conn).await()
    val consumer = ConnectionOwner.createChildActor(
      conn,
      Consumer.props(listener = Some(probe.ref)),
      timeout = 5000 millis,
      name = Some("RabbitMQ.consumer")
    )
    val deleteQueueResult = consumer ? DeleteQueue(queueName)
    Await.result(deleteQueueResult, 5 seconds)
    val deleteExchangeResult = consumer ? DeleteExchange(exchangeName)
    Await.result(deleteExchangeResult, 5 seconds)

    /**
     * Close Producer actors and connections
     */
    conn ! Close()
    Await.result(gracefulStop(conn, 5 seconds), 10 seconds)
    Await.result(gracefulStop(consumer, 5 seconds), 10 seconds)
  }

  override def afterAll : Unit = {
    system.terminate()
  }
}
