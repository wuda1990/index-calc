package com.one.piece.rm.trade.index.calc.model.result;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public class PanResultSerializer implements SerializationSchema<PanResult> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(PanResult panResult) {
        try {
            log.info("panResult:{}", panResult);
            //if topic is null, default topic will be used
            return objectMapper.writeValueAsBytes(panResult);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + panResult, e);
        }
    }
}
