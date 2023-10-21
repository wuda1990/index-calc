package com.one.piece.rm.trade.index.calc;

import com.one.piece.rm.trade.index.calc.job.PanIndexJob;
import com.one.piece.rm.trade.index.calc.model.Transaction;
import com.one.piece.rm.trade.index.calc.model.TransactionDeserializer;
import com.one.piece.rm.trade.index.calc.model.result.PanResult;
import com.one.piece.rm.trade.index.calc.model.result.PanResultSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class IndexCalcApplication {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String inputTopic = params.get("input-topic", "transactions2");
        String outputTopic = params.get("output-topic", "panResult");
        String brokers = params.get("bootstrap.servers", "localhost:9092");
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "index-calc");

        KafkaSource<Transaction> kafkaSource = KafkaSource.<Transaction>builder()
            .setTopics(inputTopic)
            .setValueOnlyDeserializer(new TransactionDeserializer())
            .setProperties(kafkaProps)
            .build();

        final KafkaSink<PanResult> kafkaSink = KafkaSink.<PanResult>builder()
            .setBootstrapServers(kafkaProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
            .setKafkaProducerConfig(kafkaProps)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(outputTopic)
                    .setValueSerializationSchema(new PanResultSerializer())
                    .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        PanIndexJob panIndexJob = new PanIndexJob();
        panIndexJob.execute(kafkaSource, kafkaSink);
        //Todo can we execute multiple jobs in one application?
    }
}
