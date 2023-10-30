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

package com.alibaba.polardbx.sequence.impl;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INCREMENT_BY;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_MAX_VALUE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_START_WITH;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NOCYCLE;

public abstract class FunctionalSequence extends BaseSequence {

    protected int incrementBy = DEFAULT_INCREMENT_BY;
    protected long startWith = DEFAULT_START_WITH;
    protected long maxValue = DEFAULT_MAX_VALUE;
    protected int cycle = NOCYCLE;

    public int getIncrementBy() {
        return incrementBy;
    }

    public void setIncrementBy(int incrementBy) {
        if (incrementBy <= 0 || incrementBy > Integer.MAX_VALUE) {
            incrementBy = 1;
        }
        this.incrementBy = incrementBy;
    }

    public long getStartWith() {
        return startWith;
    }

    public void setStartWith(long startWith) {
        if (startWith <= 0 || startWith > Long.MAX_VALUE) {
            startWith = 1;
        }
        this.startWith = startWith;
    }

    public long getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(long maxValue) {
        if (maxValue <= 0 || maxValue > Long.MAX_VALUE) {
            maxValue = Long.MAX_VALUE;
        }
        this.maxValue = maxValue;
    }

    public int getCycle() {
        return cycle;
    }

    public void setCycle(int cycle) {
        this.cycle = cycle;
    }

}
