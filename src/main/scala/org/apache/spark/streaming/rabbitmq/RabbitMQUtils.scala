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

import java.util.{List => JList, Map => JMap}

import com.rabbitmq.client.QueueingConsumer.Delivery
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaInputDStream, JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.rabbitmq.distributed.{JavaRabbitMQDistributedKey, RabbitMQDStream, RabbitMQDistributedKey}
import org.apache.spark.streaming.rabbitmq.receiver.RabbitMQInputDStream

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object RabbitMQUtils {

  /**
   * Create an input stream that receives messages from a RabbitMQ.
   *
   * @param ssc            StreamingContext object
   * @param params         RabbitMQ params
   * @param messageHandler Function to convert he raw type of the rabbitMQ messages to the type R
   */
  def createStream[R: ClassTag](
                                 ssc: StreamingContext,
                                 params: Map[String, String],
                                 messageHandler: Delivery => R
                               ): ReceiverInputDStream[R] = {
    new RabbitMQInputDStream[R](ssc, params, messageHandler)
  }

  /**
   * Create an input stream that receives messages from a RabbitMQ.
   * The result DStream is mapped to String type
   *
   * @param ssc    StreamingContext object
   * @param params RabbitMQ params
   */
  def createStream[R >: String <: String : Manifest](
                                                      ssc: StreamingContext,
                                                      params: Map[String, String]
                                                    ): ReceiverInputDStream[String] = {
    val messageHandler = (rawMessage: Delivery) => new Predef.String(rawMessage.getBody)

    new RabbitMQInputDStream[String](ssc, params, messageHandler)
  }

  /**
   * Create an input stream that receives messages from a RabbitMQ queue.
   *
   * @param javaStreamingContext JavaStreamingContext object
   * @param recordClass          Class type for R
   * @param params               RabbitMQ params
   * @param messageHandler       Function to convert he raw type of the rabbitMQ messages to the type R
   */
  def createJavaStream[R](
                           javaStreamingContext: JavaStreamingContext,
                           recordClass: Class[R],
                           params: JMap[String, String],
                           messageHandler: JFunction[Delivery, R]
                         ): JavaReceiverInputDStream[R] = {

    implicit val recordCmt: ClassTag[R] = ClassTag(recordClass)
    val cleanedHandler = javaStreamingContext.sparkContext.clean(messageHandler.call _)


    createStream[R](javaStreamingContext.ssc, params.asScala.toMap, cleanedHandler)
  }

  /**
   *
   * Create an input stream that receives messages from a RabbitMQ queue in distributed mode, each executor can
   * consume messages from a different cluster or queue-exchange. Is possible to parallelize the consumer in one
   * executor creating more channels, this is transparent to the user.
   * The result DStream is mapped to the type R with the function messageHandler.
   *
   * @param ssc             StreamingContext object
   * @param distributedKeys Object that can contains the connections to the queues, it can be more than one and each
   *                        tuple of queue, exchange, routing key and hosts can be one RabbitMQ independent
   * @param rabbitMQParams  RabbitMQ params with queue options, spark options and consumer options
   * @param messageHandler  Function to convert he raw type of the rabbitMQ messages to the type R
   * @tparam R Type or Class that the output DStream should contains for each message consumed
   * @return The new DStream with the messages consumed and parsed to the R type
   */
  def createDistributedStream[R: ClassTag](
                                            ssc: StreamingContext,
                                            distributedKeys: Seq[RabbitMQDistributedKey],
                                            rabbitMQParams: Map[String, String],
                                            messageHandler: Delivery => R
                                          ): InputDStream[R] = {
    val cleanedHandler = ssc.sc.clean(messageHandler)

    new RabbitMQDStream[R](ssc, distributedKeys, rabbitMQParams, cleanedHandler)
  }

  /**
   *
   * Create an input stream that receives messages from a RabbitMQ queue in distributed mode, each executor can
   * consume messages from a different cluster or queue-exchange. Is possible to parallelize the consumer in one
   * executor creating more channels, this is transparent to the user.
   * The result DStream is mapped to String type
   *
   * @param ssc             StreamingContext object
   * @param distributedKeys Object that can contains the connections to the queues, it can be more than one and each
   *                        tuple of queue, exchange, routing key and hosts can be one RabbitMQ independent
   * @param rabbitMQParams  RabbitMQ params with queue options, spark options and consumer options
   * @tparam R Type or Class that the output DStream should contains for each message consumed
   * @return The new DStream with the messages consumed and parsed to the String type
   */
  def createDistributedStream[R >: String <: String : Manifest](
                                                                 ssc: StreamingContext,
                                                                 distributedKeys: Seq[RabbitMQDistributedKey],
                                                                 rabbitMQParams: Map[String, String]
                                                               ): InputDStream[String] = {
    val messageHandler = (rawMessage: Delivery) => new Predef.String(rawMessage.getBody)

    new RabbitMQDStream[String](ssc, distributedKeys, rabbitMQParams, messageHandler)
  }

  /**
   *
   * Create an input stream that receives messages from a RabbitMQ queue in distributed mode, each executor can
   * consume messages from a different cluster or queue-exchange. Is possible to parallelize the consumer in one
   * executor creating more channels, this is transparent to the user.
   * The result DStream is mapped to the type R with the function messageHandler.
   *
   * @param javaStreamingContext JavaStreamingContext object
   * @param recordClass          Class type for R
   * @param distributedKeys      Object that can contains the connections to the queues, it can be more than one and each
   *                             tuple of queue, exchange, routing key and hosts can be one RabbitMQ independent
   * @param rabbitMQParams       RabbitMQ params with queue options, spark options and consumer options
   * @param messageHandler       Function to convert he raw type of the rabbitMQ messages to the type R
   * @tparam R Type or Class that the output DStream should contains for each message consumed
   * @return The new DStream with the messages consumed and parsed to the R type
   */
  def createJavaDistributedStream[R](
                                  javaStreamingContext: JavaStreamingContext,
                                  recordClass: Class[R],
                                  distributedKeys: JList[JavaRabbitMQDistributedKey],
                                  rabbitMQParams: JMap[String, String],
                                  messageHandler: JFunction[Delivery, R]
                                ): JavaInputDStream[R] = {
    implicit val recordCmt: ClassTag[R] = ClassTag(recordClass)
    val cleanedHandler = javaStreamingContext.sparkContext.clean(messageHandler.call _)
    val scalaDistributedKeys = distributedKeys.asScala.map(distributedKey =>
      RabbitMQDistributedKey(distributedKey.queue,
        distributedKey.exchangeAndRouting,
        distributedKey.connectionParams.asScala.toMap
      )
    )

    new RabbitMQDStream[R](
      javaStreamingContext.ssc,
      scalaDistributedKeys,
      rabbitMQParams.asScala.toMap,
      cleanedHandler
    )
  }
}
