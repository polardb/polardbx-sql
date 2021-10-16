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

import com.google.common.base.Preconditions;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.BufferInputBatchQueue;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicate;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.List;

public class LookupJoinGsiExec extends LookupJoinExec implements LookupTableExec {

    public LookupJoinGsiExec(Executor outerInput, Executor innerInput, JoinRelType joinType,
                             boolean maxOneRow, List<EquiJoinKey> joinKeys,
                             List<EquiJoinKey> allJoinKeys,
                             IExpression otherCondition,
                             LookupPredicate predicates,
                             ExecutionContext context,
                             int shardCount,
                             int parallelism) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeys, allJoinKeys, otherCondition, predicates, context,
            shardCount, parallelism);
        Preconditions
            .checkArgument(outerResumeExec == outerInput, "The outerResumeExec should be outerInput directly!");
        Preconditions.checkArgument(
            (outerInput instanceof LookupTableScanExec) && (innerInput instanceof LookupTableScanExec),
            "All inputs should be TableScanExec!");
    }

    @Override
    public boolean shardEnabled() {
        return false;
    }

    @Override
    public void updateLookupPredicate(Chunk chunk) {
        ((LookupTableScanExec) outerInput).updateLookupPredicate(chunk);
    }

    @Override
    public void setMemoryAllocator(MemoryAllocatorCtx memoryAllocator) {
        ((LookupTableScanExec) outerInput).setMemoryAllocator(memoryAllocator);
    }

    @Override
    public void releaseConditionMemory() {
        ((LookupTableScanExec) outerInput).releaseConditionMemory();
    }

    @Override
    protected boolean resumeAfterSuspend() {
        //LookupJoinGsiExec shouldn't resume itself, because it will resume by other operators;
        doSuspend();
        shouldSuspend = false;
        beingConsumeOuter = true;
        return false;
    }

    @Override
    public boolean resume() {
        this.isFinish = false;
        this.outerNoMoreData = false;
        this.batchQueue = new BufferInputBatchQueue(batchSize, outerInput.getDataTypes(), memoryAllocator, context);
        try {
            this.hashTable = null;
            this.positionLinks = null;
            this.beingConsumeOuter = true;
            if (bufferMemoryAllocator != null) {
                this.bufferMemoryAllocator.releaseReservedMemory(bufferMemoryAllocator.getReservedAllocated(), true);
            }
            ((LookupTableScanExec) outerInput).resume();
        } catch (Throwable t) {
            this.isFinish = true;
            throw new RuntimeException(t);
        }
        return true;
    }
}
