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

import scala.reflect.ClassTag

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object RabbitMQUtils {

  /**
   * Create an input stream that receives messages from a RabbitMQ.
   * @param ssc          StreamingContext object
   * @param params       RabbitMQ params
   */
  def createStream(ssc: StreamingContext, params: Map[String, String]): ReceiverInputDStream[String] = {
    new RabbitMQInputDStream(ssc, params)
  }

  /**
   * Create an input stream that receives messages from a RabbitMQ queue.
   * @param jssc         JavaStreamingContext object
   * @param params       RabbitMQ params
   */
  def createJavaStream(jssc: JavaStreamingContext,
                       params: Map[String, String]): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, params)
  }

}
