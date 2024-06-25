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

/**
 * iterator for single raw string
 *
 * @author fangwu
 */
public class SingleRawStringPruningIterator implements PruningRawStringStep {
    private final int pruningTime;
    private final int rawStringIndex;
    private final PruneRawString pruneRawString;
    private int current = 0;

    public SingleRawStringPruningIterator(RawString rawString, int rawStringIndex) {
        this.pruningTime = rawString.size();
        this.rawStringIndex = rawStringIndex;
        this.pruneRawString = rawString.pruneStep(0);
    }

    @Override
    public boolean hasNext() {
        return current < pruningTime;
    }

    @Override
    public Void next() {
        if (!hasNext()) {
            throw new IllegalStateException("No more elements in the iterator.");
        }
        pruneRawString.pruneStep(current);
        current++;
        return null;
    }

    public int getRawStringIndex() {
        return rawStringIndex;
    }

    public PruneRawString getPruneRawString() {
        return pruneRawString;
    }

    @Override
    public Pair<Integer, PruneRawString>[] getTargetPair() {
        return new Pair[] {Pair.of(rawStringIndex, pruneRawString)};
    }

    @Override
    public int[] getTargetIndex() {
        return new int[] {rawStringIndex};
    }
}
