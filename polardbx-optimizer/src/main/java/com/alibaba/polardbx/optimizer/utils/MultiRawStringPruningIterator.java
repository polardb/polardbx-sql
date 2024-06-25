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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.jdbc.PruneRawString;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.utils.Pair;

import java.util.Map;

/**
 * iterator for multi raw string pruning
 *
 * @author fangwu
 */
public class MultiRawStringPruningIterator implements PruningRawStringStep {
    private final int pruningTime;
    private final Pair<Integer, PruneRawString>[] pruneRawStringArr;
    private int current = 0;
    private int rawStringSize;

    public MultiRawStringPruningIterator(int pruningTime, Map<Integer, RawString> rawStringMap) {
        this.pruningTime = pruningTime;
        this.rawStringSize = rawStringMap.size();
        this.pruneRawStringArr = new Pair[rawStringSize];
        int index = 0;
        for (Map.Entry<Integer, RawString> entry : rawStringMap.entrySet()) {
            this.pruneRawStringArr[index++] = Pair.of(entry.getKey(), entry.getValue().pruneStep(0));
        }
    }

    @Override
    public boolean hasNext() {
        return current < pruningTime;
    }

    @Override
    public Void next() {
        if (!hasNext()) {
            throw new IllegalStateException("No more elements in the MultiRawStringPruningIterator.");
        }
        int totalSize = pruningTime;
        int currentIndex = current;
        for (int dim = 0; dim < rawStringSize; dim++) {
            Pair<Integer, PruneRawString> pair = pruneRawStringArr[dim];
            int rawStringSize = pair.getValue().getSourceSize();
            int otherSize = totalSize / rawStringSize;
            int curIndex = currentIndex / otherSize;
            currentIndex = currentIndex % otherSize;
            totalSize = otherSize;
            pair.getValue().pruneStep(curIndex);
        }
        current++;
        return null;
    }

    @Override
    public Pair<Integer, PruneRawString>[] getTargetPair() {
        return pruneRawStringArr;
    }

    @Override
    public int[] getTargetIndex() {
        int[] targetIndex = new int[rawStringSize];
        for (int i = 0; i < rawStringSize; i++) {
            targetIndex[i] = pruneRawStringArr[i].getKey();
        }
        return targetIndex;
    }
}
