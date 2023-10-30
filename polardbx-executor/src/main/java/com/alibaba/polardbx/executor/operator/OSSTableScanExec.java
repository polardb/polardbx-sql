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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.executor.archive.reader.OSSPhysicalTableReadResult;
import com.alibaba.polardbx.executor.archive.reader.OSSReadOption;
import com.alibaba.polardbx.executor.archive.reader.OSSTableReader;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTE_MPP;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTE_ON_MYSQL;
import static com.alibaba.polardbx.common.properties.ConnectionParams.WAIT_BLOOM_FILTER_TIMEOUT_MS;

public class OSSTableScanExec extends AbstractOSSTableScanExec {
    private List<OssSplit> splits;
    private OSSTableScan ossTableScan;
    private List<DataType> dataTypeList;
    private List<DataType<?>> filterInputTypes;
    private List<DataType<?>> filterOutputTypes;

    private List<OSSTableReader> ossTableReaders;
    private AtomicBoolean noMoreSplit;

    private List<OSSPhysicalTableReadResult> physicalTableReadResults;
    private int splitIndex;
    private int resultIndex;
    private volatile boolean isFinished;

    // for data filter
    private VectorizedExpression condition;
    private MutableChunk preAllocatedChunk;
    private int[] filterBitmap;
    private int[] outProject;
    private List<DataType<?>> inProjectDataTypeList;

    RexNode bloomFilterCondition;
    private volatile SettableFuture<?> waitBloomFilterFuture = null;
    private volatile ScheduledFuture<?> monitorWaitBloomFilterFuture = null;
    private volatile boolean needWaitBloomFilter;
    private volatile Map<Integer, BloomFilterInfo> bloomFilterInfos = null;

    private final Object lock = new Object();

    protected volatile TddlRuntimeException exception = null;

    private SessionProperties sessionProperties;

    public OSSTableScanExec(OSSTableScan ossTableScan, ExecutionContext context, List<DataType> dataTypeList) {
        super(context);
        this.splits = new ArrayList<>();
        this.ossTableScan = ossTableScan;
        this.dataTypeList = dataTypeList;
        this.ossTableReaders = new ArrayList<>();
        this.noMoreSplit = new AtomicBoolean(false);
        this.isFinished = false;
        this.physicalTableReadResults = new ArrayList<>();
        this.inProjectDataTypeList = ossTableScan.getOrcNode().getInProjectsDataType();
        this.sessionProperties = SessionProperties.fromExecutionContext(context);
    }

    @Override
    public void addSplit(Split split) {
        OssSplit ossSplit = (OssSplit) split.getConnectorSplit();
        this.splits.add(ossSplit);
    }

    @Override
    public void noMoreSplits() {
        this.noMoreSplit.set(true);
    }

    @Override
    public Integer getSourceId() {
        return ossTableScan.getRelatedId();
    }

    @Override
    Chunk doSourceNextChunk() {
        if (exception != null) {
            throw exception;
        }

        if (needWaitBloomFilter && bloomFilterInfos == null) {
            return null;
        }

        final boolean useBufferPool = context.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_BUFFER_POOL);

        // consume all splits
        for (; splitIndex < splits.size(); splitIndex++) {
            // lazy init split
            lazyInitSplit(splitIndex);

            for (; resultIndex < physicalTableReadResults.size(); resultIndex++) {
                OSSPhysicalTableReadResult readResult = physicalTableReadResults.get(resultIndex);
                Chunk chunk = null;

                // scan from oss
                if (!useBufferPool) {
                    if (condition == null) {
                        // full table scan
                        chunk = readResult.next(inProjectDataTypeList, this.blockBuilders);
                    } else {
                        // scan with filter
                        chunk = readResult
                            .next(inProjectDataTypeList, blockBuilders, condition, preAllocatedChunk, filterBitmap,
                                outProject, context);
                    }
                }

                // scan from buffer pool
                if (useBufferPool) {
                    if (condition == null) {
                        // full table scan
                        chunk = readResult.nextChunkFromBufferPool(this.blockBuilders, sessionProperties,
                            inProjectDataTypeList, outProject);
                    } else {
                        // scan with filter
                        chunk = readResult.nextChunkFromBufferPool(inProjectDataTypeList, blockBuilders, condition,
                            preAllocatedChunk,
                            filterBitmap, outProject, context);
                    }
                }
                if (chunk != null) {
                    reset();
                    return chunk;
                }
            }

            for (OSSPhysicalTableReadResult readResult : physicalTableReadResults) {
                readResult.close();
            }
            this.ossTableReaders.clear();
            this.physicalTableReadResults.clear();
        }

        isFinished = true;
        return null;
    }

    private void lazyInitSplit(int splitIndex) {
        OssSplit ossSplit = splits.get(splitIndex);
        if (!ossSplit.isInit()) {
            synchronized (lock) {
                ossSplit.init(ossTableScan, context, sessionProperties, null, null);
            }
            List<OSSReadOption> allOptions = ossSplit.getReadOptions();
            allOptions.forEach(
                option -> {
                    OSSTableReader ossTableReader = new OSSTableReader(option, context,
                        ossTableScan.getAgg(), ossTableScan.getAggColumns());
                    this.ossTableReaders.add(ossTableReader);
                }
            );

            for (OSSTableReader ossTableReader : ossTableReaders) {
                OSSPhysicalTableReadResult readResult = ossTableReader.readBatch();
                readResult.init();
                physicalTableReadResults.add(readResult);
            }
            resultIndex = 0;
        }
    }

    @Override
    void doOpen() {
        if (!noMoreSplit.get()) {
            throw new TddlRuntimeException(ERR_EXECUTE_ON_MYSQL, "input splits are not ready!");
        }

        createBlockBuilders();
        this.splitIndex = 0;
    }

    @Override
    void doClose() {
        for (OSSPhysicalTableReadResult readResult : physicalTableReadResults) {
            readResult.close();
        }
        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public List<DataType> getDataTypes() {
        return dataTypeList;
    }

    @Override
    public boolean produceIsFinished() {
        return isFinished;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        // synchronized
        return NOT_BLOCKED;
    }

    public VectorizedExpression getCondition() {
        return condition;
    }

    public void setCondition(VectorizedExpression condition) {
        this.condition = condition;
    }

    public MutableChunk getPreAllocatedChunk() {
        return preAllocatedChunk;
    }

    public void setPreAllocatedChunk(MutableChunk preAllocatedChunk) {
        this.preAllocatedChunk = preAllocatedChunk;
    }

    public void setFilterBitmap(int[] filterBitmap) {
        this.filterBitmap = filterBitmap;
    }

    public int[] getOutProject() {
        return outProject;
    }

    public void setOutProject(int[] outProject) {
        this.outProject = outProject;
    }

    public List<DataType<?>> getFilterInputTypes() {
        return filterInputTypes;
    }

    public void setFilterInputTypes(List<DataType<?>> filterInputTypes) {
        this.filterInputTypes = filterInputTypes;
    }

    public List<DataType<?>> getFilterOutputTypes() {
        return filterOutputTypes;
    }

    public void setFilterOutputTypes(List<DataType<?>> filterOutputTypes) {
        this.filterOutputTypes = filterOutputTypes;
    }

    public synchronized void initWaitFuture(ListenableFuture<List<BloomFilterInfo>> listListenableFuture) {
        if (this.waitBloomFilterFuture == null) {
            this.waitBloomFilterFuture = SettableFuture.create();

            int waitTimeout = context.getParamManager().getInt(WAIT_BLOOM_FILTER_TIMEOUT_MS);
            monitorWaitBloomFilterFuture = ServiceProvider.getInstance().getTimerTaskExecutor().schedule(() -> {
                needWaitBloomFilter = false;
            }, waitTimeout, TimeUnit.MILLISECONDS);

            listListenableFuture.addListener(
                () -> {
                    try {
                        synchronized (lock) {
                            registerBloomFilter(listListenableFuture.get());
                        }
                        waitBloomFilterFuture.set(null);
                        monitorWaitBloomFilterFuture.cancel(false);
                    } catch (Throwable t) {
                        setException(new TddlRuntimeException(ERR_EXECUTE_MPP,
                            "Failed to register bloom filter in oss table scan ", t));
                    }

                }, context.getExecutorService());

            this.needWaitBloomFilter = true;
        }
    }

    private void registerBloomFilter(List<BloomFilterInfo> bloomFilterInfos) {
        try {
            this.bloomFilterInfos = bloomFilterInfos.stream()
                .collect(Collectors.toMap(BloomFilterInfo::getId, Function.identity(), (info1, info2) -> info1));

            RexBuilder rexBuilder = ossTableScan.getCluster().getRexBuilder();

            Map<Integer, RexCall> bloomFiltersMap = ossTableScan.getBloomFiltersMap();

            if (bloomFiltersMap.size() == 1) {
                bloomFilterCondition = bloomFiltersMap.values().iterator().next();
            } else {
                bloomFilterCondition = rexBuilder
                    .makeCall(TddlOperatorTable.AND, bloomFiltersMap.values().stream().collect(Collectors.toList()));
            }

        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        }
    }

    public synchronized void setException(TddlRuntimeException exception) {
        if (this.exception == null) {
            this.exception = exception;
        }
        isFinished = true;
    }
}
