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

import com.onepiece.rm.trade.generator.model.Transaction;
import com.onepiece.rm.trade.generator.model.TransactionSerializer;
import com.onepiece.rm.trade.generator.model.TransactionSupplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Generates CSV transaction records at a rate
 */
@Slf4j
public class Producer implements Runnable, AutoCloseable {

    private volatile boolean isRunning;

    private final String brokers;

    private final String topic;

    public Producer(String brokers, String topic) {
        this.brokers = brokers;
        this.topic = topic;
        this.isRunning = true;
    }

    @Override
    public void run() {
        KafkaProducer<String, Transaction> producer = new KafkaProducer<>(getProperties());
//        MockProducer<String, Transaction> producer = new MockProducer<>(true, new StringSerializer(),
//            new TransactionSerializer());

        Throttler throttler = new Throttler(100);

        TransactionSupplier transactions = new TransactionSupplier();

        while (isRunning) {

            Transaction transaction = transactions.get();

            long millis = transaction.getTxnDt().getTime();

            ProducerRecord<String, Transaction> record =
                new ProducerRecord<>(topic, null, millis, transaction.getPan(), transaction);
            final Future<RecordMetadata> sendResult = producer.send(record);
            try {
                final RecordMetadata recordMetadata = sendResult.get();
                log.info("recordMetadata:{}", recordMetadata);

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
            try {
                throttler.throttle();
            } catch (InterruptedException e) {
                isRunning = false;
            }
        }

        producer.close();
    }

    @Override
    public void close() {
        isRunning = false;
    }

    private Properties getProperties() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TransactionSerializer.class);

        return props;
    }
}
