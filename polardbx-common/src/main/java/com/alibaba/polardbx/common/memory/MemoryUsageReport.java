package com.alibaba.polardbx.common.memory;

import java.util.Map;

public class MemoryUsageReport {
    private final Map<String, Integer> fieldSizeMap;
    private final long totalSize;
    private final String memoryUsageTree;

    public MemoryUsageReport(Map<String, Integer> fieldSizeMap, long totalSize, String memoryUsageTree) {
        this.fieldSizeMap = fieldSizeMap;
        this.totalSize = totalSize;
        this.memoryUsageTree = memoryUsageTree;
    }

    public Map<String, Integer> getFieldSizeMap() {
        return fieldSizeMap;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public String getMemoryUsageTree() {
        return memoryUsageTree;
    }
}
