package com.one.piece.rm.trade.index.calc.model;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class TransactionDeserializer implements DeserializationSchema<Transaction> {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.findAndRegisterModules();
    }

    @Override
    public Transaction deserialize(final byte[] message) throws IOException {
        return objectMapper.readValue(message, Transaction.class);
    }

    @Override
    public boolean isEndOfStream(final Transaction nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}
