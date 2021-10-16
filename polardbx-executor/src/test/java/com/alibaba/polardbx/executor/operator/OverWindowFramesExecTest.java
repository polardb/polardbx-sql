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

package com.alibaba.polardbx.executor.operator;

import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.frame.OverWindowFrame;
import com.alibaba.polardbx.executor.operator.frame.RangeSlidingOverFrame;
import com.alibaba.polardbx.executor.operator.frame.RangeUnboundedFollowingOverFrame;
import com.alibaba.polardbx.executor.operator.frame.RangeUnboundedPrecedingOverFrame;
import com.alibaba.polardbx.executor.operator.frame.RowSlidingOverFrame;
import com.alibaba.polardbx.executor.operator.frame.RowUnboundedFollowingOverFrame;
import com.alibaba.polardbx.executor.operator.frame.RowUnboundedPrecedingOverFrame;
import com.alibaba.polardbx.executor.operator.frame.UnboundedOverFrame;
import com.alibaba.polardbx.executor.operator.util.RowChunksBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Sum;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class OverWindowFramesExecTest extends BaseExecTest {
    MockExec input;

    // 第二列升序，第三列降序
    @Before
    public void prepareNormalData() {
        RowChunksBuilder rowChunksBuilder =
            RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType).
                row(null, null, 2).
                row(null, 1, 2).
                row(0, 1, 1).
                chunkBreak().
                row(0, 1, null).
                row(0, 2, null).
                chunkBreak().
                row(0, 2, null).
                chunkBreak().
                row(1, 1, null);
        input = rowChunksBuilder.buildExec();
    }

    @Test
    @Ignore
    public void testUnbounded() {
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Sum(1, false, DataTypes.LongType, -1));
        aggregators.add(new Sum(2, false, DataTypes.LongType, -1));

        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);

        OverWindowFrame[] overWindowFrames = new OverWindowFrame[] {
            new UnboundedOverFrame(aggregators.get(0)),
            new UnboundedOverFrame(aggregators.get(1))
        };
        OverWindowFramesExec overWindowExec =
            new OverWindowFramesExec(input, context, overWindowFrames, partitionIndexes);
        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.DecimalType,
                DataTypes.DecimalType)
            .row(null, null, 2, new Decimal(1, 0), new Decimal(4, 0))
            .row(null, 1, 2, new Decimal(1, 0), new Decimal(4, 0))
            .row(0, 1, 1, new Decimal(6, 0), new Decimal(1, 0))
            .row(0, 1, null, new Decimal(6, 0), new Decimal(1, 0))
            .row(0, 2, null, new Decimal(6, 0), new Decimal(1, 0))
            .row(0, 2, null, new Decimal(6, 0), new Decimal(1, 0))
            .row(1, 1, null, new Decimal(1, 0), null).build();
        execForSmpMode(overWindowExec, expects, false);
    }

    @Test
    @Ignore
    public void testRowSliding() {

        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Sum(1, false, DataTypes.LongType, -1));
        aggregators.add(new Sum(2, false, DataTypes.LongType, -1));

        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);

        OverWindowFrame[] overWindowFrames = new OverWindowFrame[] {
            new RowSlidingOverFrame(Lists.newArrayList(aggregators.get(0)), 1, 1),
            new RowSlidingOverFrame(Lists.newArrayList(aggregators.get(1)), 1, 1)
        };
        OverWindowFramesExec overWindowExec =
            new OverWindowFramesExec(input, context, overWindowFrames, partitionIndexes);
        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.DecimalType,
                DataTypes.DecimalType)
            .row(null, null, 2, new Decimal(1, 0), new Decimal(4, 0))
            .row(null, 1, 2, new Decimal(1, 0), new Decimal(4, 0))
            .row(0, 1, 1, new Decimal(2, 0), new Decimal(1, 0))
            .row(0, 1, null, new Decimal(4, 0), new Decimal(1, 0))
            .row(0, 2, null, new Decimal(5, 0), null)
            .row(0, 2, null, new Decimal(4, 0), null)
            .row(1, 1, null, new Decimal(1, 0), null).build();
        execForSmpMode(overWindowExec, expects, false);
    }

    @Test
    @Ignore
    public void testRangeSliding() {

        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Sum(1, false, DataTypes.LongType, -1));
        aggregators.add(new Sum(2, false, DataTypes.LongType, -1));

        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);

        OverWindowFrame[] overWindowFrames = new OverWindowFrame[] {
            new RangeSlidingOverFrame(Lists.newArrayList(aggregators.get(0)), 1, 1, 1, true, DataTypes.IntegerType)
        };
        OverWindowFramesExec overWindowExec =
            new OverWindowFramesExec(input, context, overWindowFrames, partitionIndexes);
        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.DecimalType,
                DataTypes.DecimalType)
            .row(null, null, 2, null, new Decimal(4, 0))
            .row(null, 1, 2, new Decimal(1, 0), new Decimal(4, 0))
            .row(0, 1, 1, new Decimal(6, 0), new Decimal(1, 0))
            .row(0, 1, null, new Decimal(6, 0), null)
            .row(0, 2, null, new Decimal(6, 0), null)
            .row(0, 2, null, new Decimal(6, 0), null)
            .row(1, 1, null, new Decimal(1, 0), null).build();
        execForSmpMode(overWindowExec, expects, false);
    }

    @Test
    @Ignore
    public void testRowUnboundedFollowing() {

        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Sum(1, false, DataTypes.LongType, -1));
        aggregators.add(new Sum(2, false, DataTypes.LongType, -1));

        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);

        OverWindowFrame[] overWindowFrames = new OverWindowFrame[] {
            new RowUnboundedFollowingOverFrame(Lists.newArrayList(aggregators.get(0)), 2),
            new RowUnboundedFollowingOverFrame(Lists.newArrayList(aggregators.get(1)), 1)
        };
        OverWindowFramesExec overWindowExec =
            new OverWindowFramesExec(input, context, overWindowFrames, partitionIndexes);
        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.DecimalType,
                DataTypes.DecimalType)
            .row(null, null, 2, new Decimal(1, 0), new Decimal(4, 0))
            .row(null, 1, 2, new Decimal(1, 0), new Decimal(4, 0))
            .row(0, 1, 1, new Decimal(6, 0), new Decimal(1, 0))
            .row(0, 1, null, new Decimal(6, 0), new Decimal(1, 0))
            .row(0, 2, null, new Decimal(6, 0), null)
            .row(0, 2, null, new Decimal(5, 0), null)
            .row(1, 1, null, new Decimal(1, 0), null).build();
        execForSmpMode(overWindowExec, expects, false);
    }

    @Test
    @Ignore
    public void testRangeUnboundedFollowing() {
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Sum(1, false, DataTypes.LongType, -1));
        aggregators.add(new Sum(2, false, DataTypes.LongType, -1));

        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);

        OverWindowFrame[] overWindowFrames = new OverWindowFrame[] {
            new RangeUnboundedFollowingOverFrame(Lists.newArrayList(aggregators.get(0)), 1, 1, true,
                DataTypes.IntegerType),
            new RangeUnboundedFollowingOverFrame(Lists.newArrayList(aggregators.get(1)), 1, 2, false,
                DataTypes.IntegerType)
        };
        OverWindowFramesExec overWindowExec =
            new OverWindowFramesExec(input, context, overWindowFrames, partitionIndexes);
        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.DecimalType,
                DataTypes.DecimalType)
            .row(null, null, 2, new Decimal(1, 0), new Decimal(4, 0))
            .row(null, 1, 2, new Decimal(1, 0), new Decimal(4, 0))
            .row(0, 1, 1, new Decimal(6, 0), new Decimal(1, 0))
            .row(0, 1, null, new Decimal(6, 0), null)
            .row(0, 2, null, new Decimal(6, 0), null)
            .row(0, 2, null, new Decimal(6, 0), null)
            .row(1, 1, null, new Decimal(1, 0), null).build();
        execForSmpMode(overWindowExec, expects, false);
    }

    @Test
    @Ignore
    public void testRowUnboundedPreceding() {

        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Sum(1, false, DataTypes.LongType, -1));
        aggregators.add(new Sum(2, false, DataTypes.LongType, -1));

        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);

        OverWindowFrame[] overWindowFrames = new OverWindowFrame[] {
            new RowUnboundedPrecedingOverFrame(Lists.newArrayList(aggregators.get(0)), 1),
            new RowUnboundedPrecedingOverFrame(Lists.newArrayList(aggregators.get(1)), 1)
        };
        OverWindowFramesExec overWindowExec =
            new OverWindowFramesExec(input, context, overWindowFrames, partitionIndexes);
        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.DecimalType,
                DataTypes.DecimalType)
            .row(null, null, 2, new Decimal(1, 0), new Decimal(4, 0))
            .row(null, 1, 2, new Decimal(1, 0), new Decimal(4, 0))
            .row(0, 1, 1, new Decimal(2, 0), new Decimal(1, 0))
            .row(0, 1, null, new Decimal(4, 0), new Decimal(1, 0))
            .row(0, 2, null, new Decimal(6, 0), new Decimal(1, 0))
            .row(0, 2, null, new Decimal(6, 0), new Decimal(1, 0))
            .row(1, 1, null, new Decimal(1, 0), null).build();
        execForSmpMode(overWindowExec, expects, false);
    }

    @Test
    @Ignore
    public void testRangeUnboundedPreceding() {

        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new Sum(1, false, DataTypes.LongType, -1));
        aggregators.add(new Sum(2, false, DataTypes.LongType, -1));

        List<Integer> partitionIndexes = new ArrayList<>();
        partitionIndexes.add(0);

        OverWindowFrame[] overWindowFrames = new OverWindowFrame[] {
            new RangeUnboundedPrecedingOverFrame(Lists.newArrayList(aggregators.get(0)), 1, 1, true,
                DataTypes.IntegerType),
            new RangeUnboundedPrecedingOverFrame(Lists.newArrayList(aggregators.get(1)), 1, 2, false,
                DataTypes.IntegerType)
        };
        OverWindowFramesExec overWindowExec =
            new OverWindowFramesExec(input, context, overWindowFrames, partitionIndexes);
        List<Chunk> expects = RowChunksBuilder
            .rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.DecimalType,
                DataTypes.DecimalType)
            .row(null, null, 2, null, new Decimal(4, 0))
            .row(null, 1, 2, new Decimal(1, 0), new Decimal(4, 0))
            .row(0, 1, 1, new Decimal(6, 0), new Decimal(1, 0))
            .row(0, 1, null, new Decimal(6, 0), new Decimal(1, 0))
            .row(0, 2, null, new Decimal(6, 0), new Decimal(1, 0))
            .row(0, 2, null, new Decimal(6, 0), new Decimal(1, 0))
            .row(1, 1, null, new Decimal(1, 0), null).build();
        execForSmpMode(overWindowExec, expects, false);
    }
}
