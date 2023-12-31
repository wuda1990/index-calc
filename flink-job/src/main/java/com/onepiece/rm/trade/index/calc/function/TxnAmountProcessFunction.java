package com.onepiece.rm.trade.index.calc.function;

import com.onepiece.rm.trade.index.calc.model.Transaction;
import com.onepiece.rm.trade.index.calc.model.result.PanResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

@Slf4j
public class TxnAmountProcessFunction extends ProcessWindowFunction<Transaction, PanResult, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Transaction> iterable, Collector<PanResult> collector)
        throws Exception {
        Transaction transaction = iterable.iterator().next();
        PanResult panResult = PanResult.builder()
            .pan(key)
            .last24HourTxnAmount(transaction.getAmount())
            .build();
        log.info("TxnAmountProcessFunction panResult:{}", panResult);
        collector.collect(panResult);
    }
}
