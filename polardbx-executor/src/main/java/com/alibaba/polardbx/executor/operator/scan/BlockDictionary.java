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

package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.operator.scan.impl.LocalBlockDictionary;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

/**
 * The dictionary stores all distinct value with implicit id.
 */
public interface BlockDictionary {
    /**
     * Get dictionary value by given id.
     *
     * @param id dictionary id.
     * @return dictionary value
     */
    Slice getValue(int id);

    /**
     * Get the size of this dictionary.
     */
    int size();

    /**
     * Get the size in bytes of this dictionary.
     */
    int sizeInBytes();

    /**
     * Encoding the dictionary into the sliceOutput.
     */
    void encoding(SliceOutput sliceOutput);

    static BlockDictionary decoding(SliceInput sliceInput) {
        // only support local block dictionary util now.
        int size = sliceInput.readInt();
        Slice[] dict = new Slice[size];
        for (int i = 0; i < size; i++) {
            int len = sliceInput.readInt();
            dict[i] = sliceInput.readSlice(len);
        }
        return new LocalBlockDictionary(dict);
    }
}
