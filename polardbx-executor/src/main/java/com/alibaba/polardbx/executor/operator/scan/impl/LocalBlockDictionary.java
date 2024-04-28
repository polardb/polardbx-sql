package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.util.Arrays;

/**
 * A local dictionary scoped in a block.
 */
public class LocalBlockDictionary implements BlockDictionary {

    public static final BlockDictionary EMPTY_DICTIONARY = new LocalBlockDictionary(new Slice[0]);

    // NOTE: the format (slice + offsets) is not efficient enough
    private final Slice[] dict;
    private final int sizeInBytes;
    private final int hashCode;

    // NOTE: we suppose that the dict array is in lexicographic order.
    public LocalBlockDictionary(Slice[] dict) {
        this.dict = dict;
        this.hashCode = Arrays.hashCode(dict);
        int sizeInBytes = 0;
        for (Slice dictValue : dict) {
            sizeInBytes += dictValue.length();
        }
        this.sizeInBytes = sizeInBytes;
    }

    public Slice[] getDict() {
        return dict;
    }

    @Override
    public Slice getValue(int id) {
        return dict[id];
    }

    @Override
    public int size() {
        return dict.length;
    }

    @Override
    public int sizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public void encoding(SliceOutput sliceOutput) {
        sliceOutput.writeInt(dict.length);
        for (Slice dictValue : dict) {
            sliceOutput.writeInt(dictValue.length());
            sliceOutput.writeBytes(dictValue);
        }
    }

    @Override
    public int hashCode() {
        return this.hashCode;
    }
}
