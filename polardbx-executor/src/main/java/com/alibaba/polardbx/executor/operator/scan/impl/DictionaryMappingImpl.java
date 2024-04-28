package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class DictionaryMappingImpl implements DictionaryMapping {
    public List<Slice> getMergedDict() {
        return mergedDict;
    }

    private List<Slice> mergedDict = new ArrayList<>();
    private Map<Integer, int[]> reMappings = new HashMap<>();

    @Override
    public int[] merge(BlockDictionary dictionary) {
        int hashCode = dictionary.hashCode();
        int[] reMapping;
        if ((reMapping = reMappings.get(hashCode)) != null) {
            return reMapping;
        }

        // merge
        reMapping = new int[dictionary.size()];
        for (int originalDictId = 0; originalDictId < dictionary.size(); originalDictId++) {
            Slice originalDictValue = dictionary.getValue(originalDictId);

            // Find the index of dict value, and record it into reMapping array.
            int index = mergedDict.indexOf(originalDictValue);
            if (index == -1) {
                mergedDict.add(originalDictValue);
                index = mergedDict.size() - 1;
            }
            reMapping[originalDictId] = index;
        }
        reMappings.put(hashCode, reMapping);
        return reMapping;
    }

    @Override
    public long estimatedSize() {
        AtomicLong estimatedSize = new AtomicLong();
        for (Slice dictValue : mergedDict) {
            estimatedSize.addAndGet(dictValue == null ? 0 : dictValue.length());
        }

        reMappings.forEach((integer, intArray) ->
            estimatedSize.addAndGet(Integer.BYTES + intArray.length * Integer.BYTES));
        return estimatedSize.get();
    }

    @Override
    public void close() {
        mergedDict.clear();
        reMappings.clear();
        mergedDict = null;
        reMappings = null;
    }
}
