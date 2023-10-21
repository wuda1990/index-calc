package com.one.piece.rm.trade.index.calc.sink;

import com.one.piece.rm.trade.index.calc.model.result.PanResult;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertSink implements SinkFunction<PanResult> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AlertSink.class);

    @Override
    public void invoke(PanResult value, Context context) {
        LOG.info(value.toString());
        System.out.println("test" + value);
    }
}
