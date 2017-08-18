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
package org.apache.spark.streaming.rabbitmq;

import com.rabbitmq.client.QueueingConsumer.Delivery;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashMap;
import java.util.Map;


public final class JavaRabbitMQConsumer {

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setAppName("JavaRabbitMQConsumer").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));
        Map<String, String> params = new HashMap<String, String>();

        params.put("hosts", "localhost");
        params.put("queueName", "rabbitmq-queue");
        params.put("exchangeName", "rabbitmq-exchange");
        params.put("vHost", "/");
        params.put("userName", "guest");
        params.put("password", "guest");

        Function<Delivery, String> messageHandler = new Function<Delivery, String>() {

            public String call(Delivery message) {
                return new String(message.getBody());
            }
        };

        JavaReceiverInputDStream<String> messages =
                RabbitMQUtils.createJavaStream(jssc, String.class, params, messageHandler);

        messages.print();

        jssc.start();
        jssc.awaitTermination();
    }
}