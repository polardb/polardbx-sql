/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import io.airlift.slice.Slice;

import java.util.HashMap;
import java.util.Map;

/**
 * A dictionary mapping for single value.
 */
public class SingleDictionaryMapping implements DictionaryMapping {
    private final Slice singleValue;

    /**
     * The size of int array is 1, and the element = -1 means no matched value exists in dictionary.
     */
    private Map<Integer, int[]> reMappings = new HashMap<>();

    public SingleDictionaryMapping(Slice singleValue) {
        this.singleValue = singleValue;
    }

    @Override
    public int[] merge(BlockDictionary dictionary) {
        int hashCode = dictionary.hashCode();
        int[] reMapping;
        if ((reMapping = reMappings.get(hashCode)) != null) {
            return reMapping;
        }

        // use int array of one slot to store the matched index of target dictionary.
        reMapping = new int[] {-1};
        for (int originalDictId = 0; originalDictId < dictionary.size(); originalDictId++) {
            Slice originalDictValue = dictionary.getValue(originalDictId);

            // Find the first matched dict value.
            if (originalDictValue.compareTo(singleValue) == 0) {
                reMapping[0] = originalDictId;
                break;
            }
        }
        reMappings.put(hashCode, reMapping);
        return reMapping;
    }

    @Override
    public void close() {
        reMappings.clear();
        reMappings = null;
    }
}
