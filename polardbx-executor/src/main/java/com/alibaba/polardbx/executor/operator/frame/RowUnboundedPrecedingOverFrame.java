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
 * The row UnboundPreceding window frame calculates frames with the following SQL form:
 * ... ROW BETWEEN UNBOUNDED PRECEDING AND [window frame following]
 * [window frame following] ::= [unsigned_value_specification] FOLLOWING | CURRENT ROW
 *
 * <p>e.g.: ... ROW BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING.
 */
public class RowUnboundedPrecedingOverFrame extends UnboundedPrecedingOverFrame {

    private int rightBound;

    private int rightIndex;
    private int currentIndex;

    public RowUnboundedPrecedingOverFrame(
        List<Aggregator> aggregators,
        int rightBound) {
//        Expression rightBound) {
        super(aggregators);
        this.rightBound = rightBound;
    }

    @Override
    public void updateIndex(int leftIndex, int rightIndex) {
        this.rightIndex = rightIndex - 1;
        currentIndex = leftIndex;
        // 每次更换partition时重置，且只需重置一次
        aggregators = aggregators.stream().map(aggregator -> aggregator.getNew()).collect(Collectors.toList());
    }

    @Override
    public List<Object> processData(int index) {
        // 因为是一直追加行，因此不需要重置window function
        // 形如，unbounded preceding and 10 following，后十行不会重复计算
        while (currentIndex <= (index + rightBound) && currentIndex <= rightIndex) {
            final int l = currentIndex++;
            aggregators.forEach(aggregator -> aggregator.aggregate(chunksIndex.rowAt(l)));
        }
        return aggregators.stream().map(t -> t.value()).collect(Collectors.toList());
    }
}
