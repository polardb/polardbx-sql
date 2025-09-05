package com.alibaba.polardbx.common.memory;

import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;

public interface FastMemoryCounter {
    int STRING_INSTANCE_SIZE = ClassLayout.parseClass(String.class).instanceSize();
    int ARRAY_LIST_INSTANCE_SIZE = ClassLayout.parseClass(ArrayList.class).instanceSize();
    int DEFAULT_MAX_DEPTH = 128;

    static MemoryUsageReport parseInstance(Object root) {
        FastMemoryCounter fastMemoryCounter = new AnnotationBasedMemoryCounter();
        return fastMemoryCounter.getMemoryUsage(root);
    }

    static MemoryUsageReport parseInstance(Object root, int maxDepth, boolean useAnnotation,
                                           boolean generateFieldSizeMap,
                                           boolean generateTreeStruct) {
        FastMemoryCounter fastMemoryCounter =
            new AnnotationBasedMemoryCounter(maxDepth, useAnnotation, generateFieldSizeMap, generateTreeStruct);
        return fastMemoryCounter.getMemoryUsage(root);
    }

    MemoryUsageReport getMemoryUsage(Object root);

    static long sizeOf(MemoryCountable memoryCountable) {
        if (memoryCountable == null) {
            return 0;
        }
        return memoryCountable.getMemoryUsage();
    }

    /**
     * Get memory usage of string.
     */
    static long sizeOf(String str) {
        if (str == null) {
            return 0;
        }
        return STRING_INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOfCharArray(str.length()));
    }

    static long sizeOf(Slice slice) {
        if (slice == null) {
            return 0;
        }
        return slice.memoryUsage();
    }

    static long sizeOf(SliceOutput sliceOutput) {
        if (sliceOutput == null) {
            return 0;
        }
        return sliceOutput.getMemoryUsage();
    }

    static long sizeOf(Blob blob, int instanceSize) {
        if (blob == null) {
            return 0;
        }
        try {
            return instanceSize + VMSupport.align((int) SizeOf.sizeOfByteArray((int) blob.length()));
        } catch (SQLException e) {
            return instanceSize;
        }
    }

    static long sizeOf(ArrayList list) {
        if (list == null) {
            return 0;
        }
        return ARRAY_LIST_INSTANCE_SIZE;
    }
}