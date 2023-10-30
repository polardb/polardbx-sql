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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.operator.factory.ExecutorFactory;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTOR;
import static com.alibaba.polardbx.common.properties.ConnectionParams.MAX_RECURSIVE_CTE_MEM_BYTES;
import static com.alibaba.polardbx.common.properties.ConnectionParams.MAX_RECURSIVE_TIME;

/**
 * Recursive Executor
 *
 * @author fangwu
 */
public class RecursiveCTEExec extends AbstractExecutor {
    private static final String CTE_PRE = "CTE_PRE";
    protected final Executor anchorExec;
    protected final ExecutorFactory recursiveFactory;

    private final long fetchSize;

    // Internal States
    private boolean isAnchorExecFinished;
    private boolean isFinished = false;
    private long currentRowIndex = 0L;

    private ListenableFuture<?> blocked;
    private Executor recursiveExecutor;
    private Executor lastRecursiveExecutorProduceData;

    private int recursiveCount;
    private long memBytes;

    private List<Chunk> chunks = Lists.newArrayList();

    private final String dataKey;

    public RecursiveCTEExec(String cteName,
                            Executor anchorExec,
                            ExecutorFactory recursiveFactory,
                            long fetchSize,
                            ExecutionContext context) {
        super(context);
        this.fetchSize = fetchSize;

        this.anchorExec = anchorExec;
        this.recursiveFactory = recursiveFactory;
        this.blocked = NOT_BLOCKED;
        this.dataKey = buildCTEKey(cteName);
    }

    @Override
    void doOpen() {
        createBlockBuilders();
        anchorExec.open();
    }

    @Override
    Chunk doNextChunk() {
        if (isFinished) {
            return null;
        }
        // loop time check
        if (recursiveCount >= InstConfUtil.getInt(MAX_RECURSIVE_TIME)) {
            isFinished = true;
            throw new TddlRuntimeException(ERR_EXECUTOR, " Recursive query aborted after " + recursiveCount
                + " iterations. Try increasing @@cte_max_recursion_depth to a larger value.");
        }

        if (!isAnchorExecFinished) {
            // process anchor exec until it finished
            blocked = anchorExec.produceIsBlocked();
            Chunk c = anchorExec.nextChunk();
            if (c == null) {
                if (anchorExec.produceIsFinished()) {
                    isAnchorExecFinished = true;
                } else {
                    return null;
                }
            } else {
                // mem check
                memBytes += c.getElementUsedBytes();
                if (memBytes > InstConfUtil.getLong(MAX_RECURSIVE_CTE_MEM_BYTES)) {
                    throw GeneralUtil.nestedException(" recursive cte mem bytes exceed" + memBytes);
                }

                chunks.add(c);

                // fetch size check
                currentRowIndex += c.getPositionCount();
                if (currentRowIndex >= fetchSize) {
                    isFinished = true;
                }
                return c;
            }
        }

        if (recursiveExecutor != null && recursiveExecutor.produceIsFinished()) {
            recursiveExecutor.close();
        }

        // recursive part
        if (recursiveExecutor == null || recursiveExecutor.produceIsFinished()) {
            buildNewRecursiveExec();
        }
        Chunk r = recursiveExecutor.nextChunk();

        if (r != null) {
            lastRecursiveExecutorProduceData = recursiveExecutor;
        }

        if (r == null) {
            // if current recursive executor never produce any row, end this recursive cte process
            if (recursiveExecutor.produceIsFinished() && lastRecursiveExecutorProduceData != recursiveExecutor) {
                isFinished = true;
            }
            return null;
        }

        // mem check, r chunk should not be null here
        memBytes += r.getElementUsedBytes();
        if (memBytes > InstConfUtil.getLong(MAX_RECURSIVE_CTE_MEM_BYTES)) {
            throw GeneralUtil.nestedException(" recursive cte mem bytes exceed" + memBytes);
        }
        chunks.add(r);
        // fetch size check
        currentRowIndex += r.getPositionCount();
        if (currentRowIndex >= fetchSize) {
            isFinished = true;
        }
        return r;
    }

    private void buildNewRecursiveExec() {
        // link data to cte anchor node
        context.getCacheRefs().put(dataKey.hashCode(), chunks);
        recursiveExecutor = recursiveFactory.createExecutor(context, 0);
        blocked = recursiveExecutor.produceIsBlocked();
        recursiveExecutor.open();
        chunks = Lists.newArrayList();
        memBytes = 0L;
        recursiveCount++;
    }

    @Override
    void doClose() {
        anchorExec.close();
        context.getCacheRefs().remove(dataKey.hashCode());
    }

    @Override
    public List<DataType> getDataTypes() {
        return anchorExec.getDataTypes();
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of(anchorExec);
    }

    @Override
    public boolean produceIsFinished() {
        return isFinished;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }

    public static String buildCTEKey(String cteName) {
        return CTE_PRE + "_" + cteName;
    }
}
