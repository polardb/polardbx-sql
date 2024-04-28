package com.alibaba.polardbx.executor.operator.scan.metrics;

import org.apache.orc.impl.StreamName;

public class MetricsNameBuilder {
    public static String streamMetricsKey(StreamName streamName, ProfileKey profileKey) {
        return String.join(".",
            profileKey.getName(),
            streamName.toString().replace(' ', '.'));
    }

    public static String columnsMetricsKey(boolean[] selectedColumns, ProfileKey profileKey) {
        return String.join(".",
            profileKey.getName(),
            "columns",
            bitmapSuffix(selectedColumns));
    }

    public static String columnMetricsKey(int targetColumnId, ProfileKey profileKey) {
        return String.join(".",
            profileKey.getName(),
            "column",
            String.valueOf(targetColumnId));
    }

    public static String bitmapSuffix(boolean[] bitmap) {
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        for (int i = 0; i < bitmap.length; i++) {
            if (!bitmap[i]) {
                continue;
            }
            builder.append(i);
            if (i != bitmap.length - 1) {
                builder.append(',');
            }
        }
        builder.append(']');
        return builder.toString();
    }
}
