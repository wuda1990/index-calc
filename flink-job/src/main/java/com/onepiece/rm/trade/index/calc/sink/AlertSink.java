package com.onepiece.rm.trade.index.calc.sink;

import com.onepiece.rm.trade.index.calc.model.result.PanResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

@Slf4j
public class AlertSink implements SinkFunction<PanResult> {

    private static final long serialVersionUID = 1L;

    @Override
    public void invoke(PanResult value, Context context) {
        log.info(value.toString());
        System.out.println("test" + value);
    }
}
