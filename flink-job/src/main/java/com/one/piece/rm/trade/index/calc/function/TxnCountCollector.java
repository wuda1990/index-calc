package com.one.piece.rm.trade.index.calc.function;

import com.one.piece.rm.trade.index.calc.model.result.PanResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
@Slf4j
public class TxnCountCollector
    extends ProcessWindowFunction<Long, PanResult, String, TimeWindow> {

    @Override
    public void process(
        final String key,
        final Context context,
        final Iterable<Long> elements,
        final Collector<PanResult> out) throws Exception {
        log.info("key:{},elements:{}", key, elements);
        Long count = elements.iterator().next();

        out.collect(PanResult.builder()
            .pan(key)
            .last24HourTxnCount(count)
            .build());
    }
}
