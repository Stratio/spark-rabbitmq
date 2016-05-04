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

package org.apache.spark.streaming.rabbitmq

import java.util.{List => JList, Map => JMap}

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.rabbitmq.distributed.{RabbitMQDStream, RabbitMQDistributedKey}
import org.apache.spark.streaming.rabbitmq.receiver.RabbitMQInputDStream

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object RabbitMQUtils {

  /**
   * Create an input stream that receives messages from a RabbitMQ.
   *
   * @param ssc    StreamingContext object
   * @param params RabbitMQ params
   */
  def createStream(ssc: StreamingContext, params: Map[String, String]): ReceiverInputDStream[String] = {
    new RabbitMQInputDStream(ssc, params)
  }

  /**
   * Create an input stream that receives messages from a RabbitMQ queue.
   *
   * @param jssc   JavaStreamingContext object
   * @param params RabbitMQ params
   */
  def createJavaStream(jssc: JavaStreamingContext,
                       params: JMap[String, String]): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, params.asScala.toMap)
  }

  def createDistributedStream[R: ClassTag](ssc: StreamingContext,
                                           distributedKeys: Seq[RabbitMQDistributedKey],
                                           rabbitMQParams: Map[String, String],
                                           messageHandler: Array[Byte] => R
                                          ): InputDStream[R] = {
    val cleanedHandler = ssc.sc.clean(messageHandler)

    new RabbitMQDStream[R](ssc, distributedKeys, rabbitMQParams, cleanedHandler)
  }

  def createDistributedStream(
                               ssc: StreamingContext,
                               distributedKeys: Seq[RabbitMQDistributedKey],
                               rabbitMQParams: Map[String, String]
                             ): InputDStream[Array[Byte]] = {
    val messageHandler = (rawMessage: Array[Byte]) => rawMessage

    new RabbitMQDStream[Array[Byte]](ssc, distributedKeys, rabbitMQParams, messageHandler)
  }

  def createDistributedStream[R >: String <: String : Manifest](
                                                                 ssc: StreamingContext,
                                                                 distributedKeys: Seq[RabbitMQDistributedKey],
                                                                 rabbitMQParams: Map[String, String]
                                                               ): InputDStream[String] = {
    val messageHandler = (rawMessage: Array[Byte]) => new Predef.String(rawMessage)

    new RabbitMQDStream[String](ssc, distributedKeys, rabbitMQParams, messageHandler)
  }

  def createDistributedStream[R](
                                  javaStreamingContext: JavaStreamingContext,
                                  recordClass: Class[R],
                                  distributedKeys: JList[RabbitMQDistributedKey],
                                  rabbitMQParams: JMap[String, String],
                                  messageHandler: JFunction[Array[Byte], R]
                                ): InputDStream[R] = {
    implicit val recordCmt: ClassTag[R] = ClassTag(recordClass)
    val cleanedHandler = javaStreamingContext.sparkContext.clean(messageHandler.call _)

    new RabbitMQDStream[R](
      javaStreamingContext.ssc,
      distributedKeys.asScala,
      rabbitMQParams.asScala.toMap,
      cleanedHandler
    )
  }
}
