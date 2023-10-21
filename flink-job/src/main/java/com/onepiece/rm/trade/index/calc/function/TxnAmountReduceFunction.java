package com.onepiece.rm.trade.index.calc.function;

import com.onepiece.rm.trade.index.calc.model.Transaction;
import org.apache.flink.api.common.functions.ReduceFunction;

public class TxnAmountReduceFunction implements ReduceFunction<Transaction> {

    @Override
    public Transaction reduce(final Transaction value1, final Transaction value2) throws Exception {
        Transaction sum = Transaction.builder()
            .txnNo(value1.getTxnNo())
            .txnDt(value1.getTxnDt())
            .amount(value1.getAmount().add(value2.getAmount()))
            .pan(value1.getPan())
            .accountId(value1.getAccountId())
            .channel(value1.getChannel())
            .build();
        return sum;
    }
}
