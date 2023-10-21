/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.onepiece.rm.trade.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * A basic data generator for continuously writing data into a Kafka topic.
 */
public class DataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("Missing parameters! \n" +
                "Usage: <KAFKA_BOOTSTRAP_SERVERS> <TOPIC>");
        }
        String KAFKA =
            Optional.ofNullable(args[0]).orElse("my-cluster-kafka-bootstrap:9092");
        String TOPIC =
            Optional.ofNullable(args[1]).orElse("transactions");

        LOG.info("Starting data generator, writing to Kafka {},Topic:{}", KAFKA, TOPIC);
        Producer producer = new Producer(KAFKA, TOPIC);

        Runtime.getRuntime()
            .addShutdownHook(
                new Thread(
                    () -> {
                        LOG.info("Shutting down");
                        producer.close();
                    }));

        producer.run();
    }
}
