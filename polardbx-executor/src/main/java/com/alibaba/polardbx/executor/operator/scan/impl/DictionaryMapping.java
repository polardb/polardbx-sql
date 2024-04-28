package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;

/**
 * Maintaining the mapping relation from all the merged dictionary.
 */
public interface DictionaryMapping {
    static DictionaryMapping create() {
        return new DictionaryMappingImpl();
    }

    int[] merge(BlockDictionary dictionary);

    default long estimatedSize() {
        return 0;
    }

    void close();
}
