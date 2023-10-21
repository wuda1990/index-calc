package com.one.piece.rm.trade.index.calc.job;

import com.one.piece.rm.trade.index.calc.function.TxnAmountProcessFunction;
import com.one.piece.rm.trade.index.calc.function.TxnAmountReduceFunction;
import com.one.piece.rm.trade.index.calc.function.TxnCountAggregator;
import com.one.piece.rm.trade.index.calc.function.TxnCountCollector;
import com.one.piece.rm.trade.index.calc.model.Transaction;
import com.one.piece.rm.trade.index.calc.model.result.PanResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

@Slf4j
public class PanIndexJob {

    public JobExecutionResult execute(KafkaSource<Transaction> kafkaSource, KafkaSink<PanResult> sink)
        throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // define warkmark strategy
        WatermarkStrategy<Transaction> watermarkStrategy = WatermarkStrategy
            .<Transaction>forBoundedOutOfOrderness(Duration.ofMillis(200))
            .withTimestampAssigner(
                (transaction, l) -> transaction.getTxnDt().getTime());

        // set up the pipeline
        final WindowedStream<Transaction, String, TimeWindow> window = env.fromSource(kafkaSource, watermarkStrategy,
                "transactions")
            .keyBy(Transaction::getPan)
            .window(TumblingEventTimeWindows.of(Time.days(3)));
        window.reduce(new TxnAmountReduceFunction(), new TxnAmountProcessFunction()).sinkTo(sink);
        //calculate the txn count for each pan in last 24 hours
        window.aggregate(new TxnCountAggregator(), new TxnCountCollector()).sinkTo(sink);

        // run the pipeline and return the result
        log.info("execute job");
        return env.execute("Pan Index Job");
    }
}
