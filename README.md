# RabbitMQ-Receiver

RabbitMQ-Receiver is a library that allows the user to read data with [Apache Spark](https://spark.apache.org/)
from [RabbitMQ](https://www.rabbitmq.com/).

## Requirements

This library requires Spark 1.3+, Scala 10+, RabbitMQ 3.5+

## Using the library

For using RabbitMQ-Receiver library you have to clone the repo an install the jar in your maven local repo by doing:

```
git clone https://github.com/Stratio/RabbitMQ-Receiver.git
mvn clean install
```

### Scala API

There are two ways of creating a receiver. Consuming data directly from a RabbitMQ queue or consuming data from a 
RabbitMQ queue through a direct exchange:

```
val receiverStream = RabbitMQUtils.createStreamFromAQueue(sparkStreamingContext, 
                                                          rabbitMQHost, 
                                                          rabbitMQPort, 
                                                          rabbitMQQueueName, 
                                                          storageLevel)
```

```
val receiverStream 
  = RabbitMQUtils.createStreamFromRoutingKeys(sparkStreamingContext, 
                                              rabbitMQHost, 
                                              rabbitMQPort, 
                                              exchangeName, 
                                              Seq("routingKey1", "routingKey2", ...), 
                                              storageLevel)
```

### Java API

As in the Scala API there are two ways of creating a receiver. Consuming data directly from a RabbitMQ queue or 
consuming data from a RabbitMQ queue through a direct exchange:

```
JavaReceiverInputDStream receiverStream 
  = RabbitMQUtils.createJavaStreamFromAQueue(javaSparkStreamingContext,
                                             rabbitMQHost, 
                                             rabbitMQPort,
                                             rabbitMQQueueName, 
                                             storageLevel);
```

```
JavaReceiverInputDStream receiverStream 
  = RabbitMQUtils.createJavaStreamFromRoutingKeys(javaSparkStreamingContext, 
                                              rabbitMQHost, 
                                              rabbitMQPort, 
                                              exchangeName, 
                                              routingKeyList, 
                                              storageLevel)
```





# License #

Licensed to STRATIO (C) under one or more contributor license agreements.
See the NOTICE file distributed with this work for additional information
regarding copyright ownership.  The STRATIO (C) licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
