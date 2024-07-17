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
import java.util.stream.Collectors;

/**
 * The row unboundedFollowing window frame calculates frames with the following SQL form:
 * ... ROW BETWEEN [window frame preceding] AND UNBOUNDED FOLLOWING
 * [window frame preceding] ::= [unsigned_value_specification] PRECEDING | CURRENT ROW
 *
 * <p>e.g.: ... ROW BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING.
 */
public class RowUnboundedFollowingOverFrame extends UnboundedFollowingOverFrame {

    private int leftBound;
    private int leftIndex;
    private int rightIndex;
    private boolean currentFrame;

    public RowUnboundedFollowingOverFrame(
        List<Aggregator> aggregator,
        int leftBound) {
        super(aggregator);
        this.leftBound = leftBound;
    }

    @Override
    public void updateIndex(int leftIndex, int rightIndex) {
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex - 1;
        this.currentFrame = false;
    }

    @Override
    public List<Object> processData(int index) {
        // 比如 10 preceding and unbounded following，则前十行的处理结果是相同的；必须加状态判断，避免滑动到上一个partition
        if (currentFrame && index - leftBound <= leftIndex) {
            return aggregators.stream().map(t -> t.value()).collect(Collectors.toList());
        }
        currentFrame = true;
        int realLeftIndex = Math.max(leftIndex, index - leftBound);
        aggregators = aggregators.stream().map(t -> t.getNew()).collect(Collectors.toList());
        for (int i = realLeftIndex; i <= rightIndex; i++) {
            final int l = i;
            aggregators.forEach(aggregator -> aggregator.aggregate(chunksIndex.rowAt(l)));
        }
        return aggregators.stream().map(t -> t.value()).collect(Collectors.toList());
    }
}

