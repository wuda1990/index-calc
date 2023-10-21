package com.one.piece.rm.trade.index.calc.function;

import com.one.piece.rm.trade.index.calc.model.Transaction;
import org.apache.flink.api.common.functions.AggregateFunction;

public class TxnCountAggregator implements AggregateFunction<Transaction, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(final Transaction value, final Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(final Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(final Long a, final Long b) {
        return a + b;
    }
}
