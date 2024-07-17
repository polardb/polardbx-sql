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

package com.alibaba.polardbx.executor.operator.frame;

import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;

import java.util.List;

/**
 * The row sliding window frame calculates frames with the following SQL form:
 * ... ROW BETWEEN [window frame preceding] AND [window frame following]
 * [window frame preceding] ::= [unsigned_value_specification] PRECEDING | CURRENT ROW
 * [window frame following] ::= [unsigned_value_specification] FOLLOWING | CURRENT ROW
 *
 * <p>e.g.: ... ROW BETWEEN 1 PRECEDING AND 1 FOLLOWING.
 */
public class RowSlidingOverFrame extends SlidingOverFrame {

    private final int leftBound;
    private final int rightBound;

    // 当前partition在chunk中的边界index
    private int rightIndex = 0;
    private int leftIndex = 0;

    public RowSlidingOverFrame(
        List<Aggregator> aggregator,
        int leftBound,
        int rightBound) {
        super(aggregator);
        this.leftBound = leftBound;
        this.rightBound = rightBound;
    }

    @Override
    public void updateIndex(int leftIndex, int rightIndex) {
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex - 1;
        prevLeftIndex = -1;
        prevRightIndex = -1;
    }

    @Override
    public List<Object> processData(int index) {
        int realLeftIndex = Math.max(leftIndex, index - leftBound);
        int realRightIndex = Math.min(rightIndex, index + rightBound);
        return process(realLeftIndex, realRightIndex);
    }
}

