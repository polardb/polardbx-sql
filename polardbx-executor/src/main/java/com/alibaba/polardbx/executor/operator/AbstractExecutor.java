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
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.optimizer.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.chunk.BlockBuilders;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.operator.EmptyExecutor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.alibaba.polardbx.statistics.RuntimeStatistics.OperatorStatisticsGroup;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Abstract Executor
 *
 */
public abstract class AbstractExecutor implements Executor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractExecutor.class);

    final ExecutionContext context;
    protected int chunkLimit;
    BlockBuilder[] blockBuilders;
    protected boolean opened = false;
    private int relId;

    protected OperatorStatistics statistics = new OperatorStatistics();
    protected long startTime = -1;
    protected final boolean enableCpuProfile;
    protected final String executorName;
    // Use For stat the time cost for fetch jdbc resultSet
    protected long startTimeCostNano = 0;

    protected long spillCnt = 0;

    /**
     * the stat group of the plan of current exec executor.
     * <p>
     * <pre>
     *  Note:
     *      When the exec executor is in apply subQuery or in scalar subQuery,
     *      the subQuery operator cpu stat will be ignore and
     *      the value of targetPlanStatGroup will be null.
     * So, must check NULL before use the value of targetPlanStatGroup.
     *
     * <pre/>
     */
    protected OperatorStatisticsGroup targetPlanStatGroup = null;

    public AbstractExecutor(ExecutionContext context) {
        this.context = context;
        this.chunkLimit = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
        this.executorName = this.getClass().getSimpleName() + "@" + System.identityHashCode(this);
        this.enableCpuProfile = ExecUtils.isOperatorMetricEnabled(context);
    }

    abstract void doOpen();

    abstract void doClose();

    abstract Chunk doNextChunk();

    @Override
    public final synchronized void open() {
        Preconditions.checkState(!opened, "operator already opened");

        try {
            beforeOpen();
            doOpen();
            afterOpen();

        } finally {
            opened = true;
        }
    }

    @Override
    public final synchronized void close() {
        // ensure idempotent and exception-free
        if (opened) {
            try {
                beforeClose();
                doClose();
                afterClose();
            } catch (Throwable ex) {
                logger.error("Failed to close operator", ex);
            }
            opened = false;
        }
    }

    @Override
    public final Chunk nextChunk() {
        Preconditions.checkState(opened, "exec not opened");

        beforeProcess();
        final Chunk ret = doNextChunk();
        afterProcess(ret);

        return ret;
    }

    final void createBlockBuilders() {
        // Create all block builders by default
        final List<DataType> columns = getDataTypes();
        blockBuilders = new BlockBuilder[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            blockBuilders[i] = BlockBuilders.create(columns.get(i), context);
        }
    }

    final Chunk buildChunkAndReset() {
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilder();
        }
        return new Chunk(blocks);
    }

    final int currentPosition() {
        return blockBuilders[0].getPositionCount();
    }

    protected void beforeOpen() {
        if (statistics != null && enableCpuProfile) {
            assert startTime <= 0;
            startTime = ThreadCpuStatUtil.getThreadCpuTimeNano();
        }
    }

    protected void afterOpen() {
        if (statistics != null && enableCpuProfile) {
            assert startTime > 0;
            long duration = ThreadCpuStatUtil.getThreadCpuTimeNano() - startTime;
            startTime = 0;
            statistics.addStartupDuration(duration);

        }
    }

    protected void beforeProcess() {
        if (statistics != null && enableCpuProfile) {
            assert startTime <= 0;
            startTime = ThreadCpuStatUtil.getThreadCpuTimeNano();
        }
    }

    protected void afterProcess(Chunk result) {
        if (statistics != null && enableCpuProfile) {
            assert startTime > 0;
            long duration = ThreadCpuStatUtil.getThreadCpuTimeNano() - startTime;
            startTime = 0;
            statistics.addProcessDuration(duration);
            if (result != null) {
                statistics.addRowCount(result.getPositionCount());
            }
        }
    }

    /**
     * An operator should records its memory usage (bytes) if it buffers any data
     */
    void collectMemoryUsage(MemoryPool pool) {
        if (statistics != null && !pool.isDestoryed()) {
            statistics.addMemory(pool.getMaxMemoryUsage());
        }
    }

    void addSpillCnt(int spillCnt) {
        this.spillCnt += spillCnt;
        checkState(this.spillCnt < 100, "The spill count of operator exceed than 100!");
        if (statistics != null) {
            statistics.addSpillCnt(spillCnt);
        }
    }

    @Override
    public void setId(int id) {
        this.relId = id;
    }

    @Override
    public int getId() {
        return relId;
    }

    protected void beforeClose() {
        if (statistics != null && enableCpuProfile) {
            startTime = ThreadCpuStatUtil.getThreadCpuTimeNano();
        }
    }

    protected void afterClose() {
        if (statistics != null && enableCpuProfile) {
            assert startTime > 0;
            statistics.addCloseDuration(ThreadCpuStatUtil.getThreadCpuTimeNano() - startTime);
            startTime = 0;
            if (this.targetPlanStatGroup != null) {
                RuntimeStatHelper.addAsyncTaskCpuTimeToParent(this.targetPlanStatGroup);
            }
        }
    }

    public BlockBuilder[] getBlockBuilders() {
        return blockBuilders;
    }

    public void setBlockBuilders(BlockBuilder[] blockBuilders) {
        this.blockBuilders = blockBuilders;
    }

    public OperatorStatistics getStatistics() {
        return statistics;
    }

    public void setStatistics(OperatorStatistics statistics) {
        this.statistics = statistics;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public OperatorStatisticsGroup getTargetPlanStatGroup() {
        return targetPlanStatGroup;
    }

    public void setTargetPlanStatGroup(OperatorStatisticsGroup targetPlanStatGroup) {
        this.targetPlanStatGroup = targetPlanStatGroup;

        List<Executor> inputExecList = new ArrayList<>();
        try {
            inputExecList = getInputs();
        } catch (Throwable ex) {
            // ignore
        }
        if (this instanceof MergeSortExec) {
            //ignore
        } else if (inputExecList == null || inputExecList.isEmpty() || inputExecList.get(0) instanceof EmptyExecutor) {
            this.targetPlanStatGroup.hasInputOperator = false;
        }
    }

    public String getExecutorName() {
        return executorName;
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }
}
