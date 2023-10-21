package com.onepiece.rm.trade.index.calc.job;

import com.onepiece.rm.trade.index.calc.function.TxnAmountProcessFunction;
import com.onepiece.rm.trade.index.calc.function.TxnAmountReduceFunction;
import com.onepiece.rm.trade.index.calc.function.TxnCountAggregator;
import com.onepiece.rm.trade.index.calc.function.TxnCountCollector;
import com.onepiece.rm.trade.index.calc.model.Transaction;
import com.onepiece.rm.trade.index.calc.model.result.PanResult;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class PanCrap {

    private SourceFunction<Transaction> sourceFunction;

    private SinkFunction<PanResult> sink;

    public PanCrap(SourceFunction<Transaction> sourceFunction, SinkFunction<PanResult> sink) {
        this.sourceFunction = sourceFunction;
        this.sink = sink;
    }

    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // define warkmark strategy
        WatermarkStrategy<Transaction> watermarkStrategy = WatermarkStrategy
            .<Transaction>forBoundedOutOfOrderness(Duration.ofMillis(200))
            .withTimestampAssigner(
                (transaction, l) -> transaction.getTxnDt().getTime());

        // set up the pipeline
        final WindowedStream<Transaction, String, TimeWindow> window = env.addSource(sourceFunction)
            .assignTimestampsAndWatermarks(watermarkStrategy)
            .keyBy(Transaction::getPan)
            .window(TumblingEventTimeWindows.of(Time.days(3)));
        window.reduce(new TxnAmountReduceFunction(), new TxnAmountProcessFunction()).addSink(sink);
        //calculate the txn count for each pan in last 24 hours
        window.aggregate(new TxnCountAggregator(), new TxnCountCollector()).addSink(sink);

        // run the pipeline and return the result
        return env.execute("PAN Crap");
    }
}
