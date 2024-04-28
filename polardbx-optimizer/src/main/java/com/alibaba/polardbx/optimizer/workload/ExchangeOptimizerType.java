package com.alibaba.polardbx.optimizer.workload;

import com.alibaba.polardbx.druid.util.StringUtils;

/**
 * @author dylan
 */
public enum ExchangeOptimizerType {
    SMP, MPP, COLUMNAR;

    public static ExchangeOptimizerType getType(String type) {
        if (StringUtils.isEmpty(type)) {
            return null;
        }
        type = type.toLowerCase();
        switch (type.toLowerCase()) {
        case "smp":
            return SMP;
        case "mpp":
            return MPP;
        case "columnar":
            return COLUMNAR;
        default:
            return null;
        }
    }
}