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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BooleanBlockBuilder;
import com.alibaba.polardbx.executor.chunk.DateBlockBuilder;
import com.alibaba.polardbx.executor.chunk.DoubleBlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.executor.chunk.StringBlockBuilder;
import com.alibaba.polardbx.executor.chunk.TimestampBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;

import java.sql.Date;
import java.sql.Timestamp;

public final class BlockAssertions {
    private BlockAssertions() {
    }

    public static Block createLongSequenceBlock(int start, int end) {
        LongBlockBuilder builder = new LongBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            builder.writeLong(i);
        }

        return builder.build();
    }

    public static Block createDoubleSequenceBlock(int start, int end) {
        DoubleBlockBuilder builder = new DoubleBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            builder.writeDouble(i);
        }

        return builder.build();
    }

    public static Block createBooleanSequenceBlock(int start, int end) {
        BooleanBlockBuilder builder = new BooleanBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            builder.writeBoolean(i % 2 == 0);
        }

        return builder.build();
    }

    public static Block createDateSequenceBlock(int start, int end) {
        DateBlockBuilder builder = new DateBlockBuilder(end - start, new DateType(), new ExecutionContext());

        for (int i = start; i < end; i++) {
            builder.writeDate(new Date(i));
        }

        return builder.build();
    }

    public static Block createTimestampSequenceBlock(int start, int end) {
        TimestampBlockBuilder builder =
            new TimestampBlockBuilder(end - start, new TimestampType(0), new ExecutionContext());

        for (int i = start; i < end; i++) {
            builder.writeTimestamp(new Timestamp(i));
        }

        return builder.build();
    }

    public static Block createStringSequenceBlock(int start, int end) {

        StringBlockBuilder builder = new StringBlockBuilder(end - start, 4);

        for (int i = start; i < end; i++) {
            builder.writeString("" + i);
        }

        return builder.build();
    }
}