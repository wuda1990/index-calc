package com.one.piece.rm.trade.index.calc.model.result;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Index calculation result for a PAN.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PanResult {

    private String pan;
    private BigDecimal last24HourTxnAmount;
    private Long last24HourTxnCount;

    @Override
    public String toString() {
        return "PanResult{" +
            "pan='" + pan + '\'' +
            ", last24HourTxnAmount=" + last24HourTxnAmount +
            ", last24HourTxnCount=" + last24HourTxnCount +
            '}';
    }
}
