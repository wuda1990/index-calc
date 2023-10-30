package com.onepiece.rm.trade.index.calc.job;

import com.onepiece.rm.trade.index.calc.function.TxnAmountProcessFunction;
import com.onepiece.rm.trade.index.calc.function.TxnAmountReduceFunction;
import com.onepiece.rm.trade.index.calc.function.TxnCountAggregator;
import com.onepiece.rm.trade.index.calc.function.TxnCountCollector;
import com.onepiece.rm.trade.index.calc.model.Transaction;
import com.onepiece.rm.trade.index.calc.model.result.PanResult;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class PanIndexJob {

    private static final Logger logger = LoggerFactory.getLogger(PanIndexJob.class);

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
            .window(TumblingEventTimeWindows.of(Time.days(3))); // should we use sliding time window?
        final SingleOutputStreamOperator<PanResult> reduce = window.reduce(new TxnAmountReduceFunction(),
            new TxnAmountProcessFunction());
        reduce.print();
        reduce.sinkTo(sink);
        //calculate the txn count for each pan in last 24 hours
        window.aggregate(new TxnCountAggregator(), new TxnCountCollector()).sinkTo(sink);

        // run the pipeline and return the result
        logger.info("execute job");
        return env.execute("Pan Index Job");
    }
}
