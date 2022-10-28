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

import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.RowChunksBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.DefaultMemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import static com.alibaba.polardbx.executor.operator.util.RowChunksBuilder.rowChunksBuilder;

public class CacheSortedRowTest {

    public void testBase(MemoryPool memoryPool) throws SQLException {
        DefaultMemoryAllocatorCtx ctx = new DefaultMemoryAllocatorCtx(memoryPool);
        DataType[] dataTypes = new DataType[] {DataTypes.StringType, DataTypes.LongType};
        ResumeTableScanSortExec.CacheSortedRow cacheSortedRow
            = new ResumeTableScanSortExec.CacheSortedRow(ctx, dataTypes, new ExecutionContext());

        RowChunksBuilder innerInputBuilder = rowChunksBuilder(dataTypes)
            .addSequenceChunk(4, 20, 200)
            .addSequenceChunk(4, 20, 200)
            .addSequenceChunk(4, 30, 300)
            .addSequenceChunk(4, 40, 400)
            .addSequenceChunk(20, 80, 123_000)
            .addSequenceChunk(100, 100, 123_000)
            .row(null, 123_000L)
            .chunkBreak();

        List<Chunk> chunks = innerInputBuilder.build();
        int expectRowNum = 0;
        for (Chunk chunk : chunks) {
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                cacheSortedRow.addRow(chunk.rowAt(i));
                expectRowNum++;
            }
        }
        Iterator<Row> iterator = cacheSortedRow.getSortedRows();

        int actualRowNum = 0;
        while (iterator.hasNext()) {
            actualRowNum++;
            iterator.next();
        }

        Assert.assertEquals(expectRowNum, actualRowNum);
        Assert.assertEquals(0, cacheSortedRow.getChunks().size());
    }

    @Test
    public void testLargeMemoryPool() throws SQLException {
        MemoryPool root = new MemoryPool("root", Integer.MAX_VALUE, MemoryType.OTHER);
        testBase(root);
    }

    @Test(expected = MemoryNotEnoughException.class)
    public void testLimitedMemoryPool() throws SQLException {
        MemoryPool root = new MemoryPool("root", 10240, MemoryType.OTHER);
        testBase(root);
    }
}
