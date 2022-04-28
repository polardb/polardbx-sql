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
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.predicate.OSSPredicateBuilder;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.executor.archive.reader.ORCReadResult;
import com.alibaba.polardbx.executor.archive.reader.OSSReadOption;
import com.alibaba.polardbx.executor.archive.reader.UnPushableORCReaderTask;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.sarg.SearchArgument;
import org.apache.orc.sarg.SearchArgumentFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTE_MPP;
import static com.alibaba.polardbx.common.properties.ConnectionParams.WAIT_BLOOM_FILTER_TIMEOUT_MS;
import static com.alibaba.polardbx.executor.operator.ProducerExecutor.NOT_BLOCKED;

public class OSSTableScanClient implements Closeable {
    public static final Logger LOGGER = LoggerFactory.getLogger(OSSTableScanClient.class);
    private static final int POOL_SIZE = 2;
    public static final int TIMEOUT = 8000;

    private OSSTableClientInitializer initializer;

    private List<OssSplit> splits;
    private int splitIndex;

    private Pool<VectorizedRowBatch> pool;
    private ConcurrentLinkedQueue<ResultFromOSS> results;

    private ExecutionContext context;
    private SessionProperties sessionProperties;
    private final int chunkLimit;

    private volatile SettableFuture<?> waitBloomFilterFuture = null;
    private volatile boolean needWaitBloomFilter;

    private boolean isFinished;
    private boolean isClosed;
    protected TddlRuntimeException exception = null;

    private final List<SettableFuture<?>> blockedCallers = new ArrayList<>();
    private PrefetchThread prefetchThread;
    private List<UnPushableORCReaderTask> registeredOrcTasks = new ArrayList<>();

    // used for agg statistics
    private List<DataType> dataTypeList;
    private List<AggregateCall> aggCalls;
    private List<RelColumnOrigin> aggColumns;

    public OSSTableScanClient(OSSTableScan ossTableScan, ExecutionContext context, List<DataType> dataTypeList) {
        this.splits = new ArrayList<>();
        this.splitIndex = 0;
        this.context = context;

        this.sessionProperties = SessionProperties.fromExecutionContext(context);
        this.chunkLimit = (int) context.getParamManager().getLong(ConnectionParams.OSS_ORC_INDEX_STRIDE);

        this.pool = new Pool<>(POOL_SIZE, TIMEOUT);
        this.results = new ConcurrentLinkedQueue<>();

        this.isFinished = false;
        this.isClosed = false;

        this.initializer = new OSSTableClientInitializer(ossTableScan);
        this.initializer.initialSearchArgs();
        this.dataTypeList = dataTypeList;
        this.aggCalls = ossTableScan.getAgg() == null ? null : ossTableScan.getAgg().getAggCallList();
        this.aggColumns = ossTableScan.getAggColumns();
    }

    public synchronized void initWaitFuture(ListenableFuture<List<BloomFilterInfo>> listListenableFuture) {
        this.initializer.initWaitFuture(listListenableFuture);
    }

    public synchronized void executePrefetchThread() {
        if (isClosed) {
            return;
        }
        throwIfFailed();
        if (needWaitBloomFilter && waitBloomFilterFuture != null && !waitBloomFilterFuture.isDone()) {
            return;
        }

        // For all split, build orc read task and fetch the results.
        this.prefetchThread = new PrefetchThread();

        //use server thread pool to submit all prefetch tasks.
        ServiceProvider.getInstance().getServerExecutor().submit(context.getSchemaName(),
            context.getTraceId(), -1, prefetchThread, null, context.getRuntimeStatistics());
    }

    public void addSplit(OssSplit split) {
        this.splits.add(split);
    }

    private synchronized void setException(TddlRuntimeException exception) {
        if (this.exception == null) {
            this.exception = exception;
        }
        cancelPrefetch();
        isClosed = true;
        notifyBlockedCallers();
    }

    private synchronized void cancelPrefetch() {
        // cancel all thread by changing the state.
        this.prefetchThread.cancel();
    }

    public synchronized ResultFromOSS popResult() {
        return results.poll();
    }

    public synchronized void recycle(ResultFromOSS result) throws InterruptedException {
        pool.recycle(result.getBatch());
    }

    public synchronized void recycle(VectorizedRowBatch batch) throws InterruptedException {
        pool.recycle(batch);
    }

    public synchronized ListenableFuture<?> isBlocked() {
        throwIfFailed();
        if (isFinished() || isFailed() || isReady()) {
            notifyBlockedCallers();
            return NOT_BLOCKED;
        } else {
            if (needWaitBloomFilter && waitBloomFilterFuture != null && !waitBloomFilterFuture.isDone()) {
                blockedCallers.add(waitBloomFilterFuture);
                return waitBloomFilterFuture;
            } else {
                SettableFuture<?> future = SettableFuture.create();
                blockedCallers.add(future);
                return future;
            }
        }
    }

    private boolean isReady() {
        return results.peek() != null || splits.isEmpty();
    }

    private boolean isFailed() {
        return exception != null;
    }

    public boolean isFinished() {
        throwIfFailed();
        return isClosed || isFinished;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void throwIfFailed() {
        if (exception != null) {
            throw GeneralUtil.nestedException(exception);
        }
    }

    private synchronized void notifyBlockedCallers() {
        // notify all futures in list.
        for (int i = 0; i < blockedCallers.size(); i++) {
            SettableFuture<?> blockedCaller = blockedCallers.get(i);
            blockedCaller.set(null);
        }
        blockedCallers.clear();
    }

    @Override
    public void close() {
        for (UnPushableORCReaderTask orcReaderTask : registeredOrcTasks) {
            orcReaderTask.close();
        }
        notifyBlockedCallers();
    }

    private class PrefetchThread implements Callable<Object> {
        private volatile boolean isCancelled;

        PrefetchThread() {
            this.isCancelled = false;
        }

        public void cancel() {
            isCancelled = true;
        }

        @Override
        public Object call() {
            try {
                // For normal case, there is only one single split allocated to a table scan exec.
                // split -> physical table -> file
                for (; splitIndex < splits.size(); splitIndex++) {

                    // initial the split (e.g. orc pruning)
                    List<OSSReadOption> readOptions = initializer.lazyInitSplit(splitIndex);
                    for (OSSReadOption readOption : readOptions) {
                        for (int i = 0; i < readOption.getTableFileList().size(); i++) {
                            // supply initial elements to pool.
                            pool.supply(() -> readOption.getReadSchema().createRowBatch(chunkLimit));
                            // do fetch file rows.
                            foreachFile(readOption, i);
                        }
                    }
                }
                // If physical table list or file list is empty, notify the blocked thread there.
                notifyBlockedCallers();
                isFinished = true;
            } catch (Throwable t) {
                setException(new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, t));
            }

            // whatever
            return null;
        }

        private void foreachFile(OSSReadOption readOption, int fileIndex) {
            String tableFile = readOption.getTableFileList().get(fileIndex);
            FileMeta fileMeta = readOption.getPhyTableFileMetas().get(fileIndex);
            PruningResult pruningResult = readOption.getPruningResultList().get(fileIndex);

            // build orc reader task for each file.
            UnPushableORCReaderTask task =
                new UnPushableORCReaderTask(readOption, tableFile, fileMeta, pruningResult, context,
                    dataTypeList, aggCalls, aggColumns);
            task.init();

            // register for resource management.
            registeredOrcTasks.add(task);

            // fetch all rows from one file.
            while (!isCancelled) {
                VectorizedRowBatch batch = null;
                boolean needRecycle = true;
                try {
                    batch = pool.poll();
                    if (batch == null) {
                        throw GeneralUtil.nestedException(String.format("cannot fetch file data in %s ms.", TIMEOUT));
                    }

                    // fetch.
                    ORCReadResult readResult = task.next(batch, sessionProperties);
                    if (readResult.getResultRows() == 0) {
                        // no more rows from this file. Try next file.
                        return;
                    }
                    // the result chunk comes from statistics
                    if (readResult.getChunk() != null) {
                        // need to recycle the batch
                        results.add(new ResultFromOSS(readResult.getChunk()));
                    } else {
                        // Fill the result and notify the block callers.
                        // Do not recycle the batch.
                        needRecycle = false;
                        results.add(new ResultFromOSS(batch));
                    }
                    notifyBlockedCallers();
                } catch (Throwable t) {
                    setException(new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, t, t.getMessage()));
                } finally {
                    if (batch != null && needRecycle) {
                        try {
                            pool.recycle(batch);
                        } catch (InterruptedException e) {
                            setException(new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, e, e.getMessage()));
                        }
                    }
                }
            }
        }
    }


    /**
     * The Pool hold all the buffer in a Prefetch thread.
     *
     * @param <T> buffer type.
     */
    private static class Pool<T> {
        private final ArrayBlockingQueue<T> pool;

        private final int poolSize;

        /**
         * Timeout in millis
         */
        private long timeout;
        private volatile boolean initialized;

        public Pool(int poolSize, long timeout) {
            this.pool = new ArrayBlockingQueue<>(poolSize);
            this.poolSize = poolSize;
            this.timeout = timeout;
            this.initialized = false;
        }

        public void supply(Supplier<T> supplier) {
            int n = this.poolSize;
            while (!initialized && n-- > 0) {
                this.pool.add(supplier.get());
            }
            initialized = true;
        }

        public T poll() throws InterruptedException {
            // When the consumer is slower than producer.
            // Wait for timeout-milliseconds
            return this.pool.poll(timeout, TimeUnit.MILLISECONDS);
        }

        public void recycle(T element) throws InterruptedException {
            // There will be 0, 1, ... n-1 elements in n-size pool. (at least 1 element has been fetched.)
            // So it's impossible to fail to recycle elements.
            this.pool.offer(element, timeout, TimeUnit.MILLISECONDS);
        }
    }

    private class OSSTableClientInitializer {
        private OSSTableScan ossTableScan;
        private SearchArgument searchArgument;
        private String[] columns;
        private volatile Map<Integer, BloomFilterInfo> bloomFilterInfos;
        private volatile ScheduledFuture<?> monitorWaitBloomFilterFuture;
        private final Object lock = new Object();

        OSSTableClientInitializer(OSSTableScan ossTableScan) {
            this.ossTableScan = ossTableScan;
        }

        public List<OSSReadOption> lazyInitSplit(int splitIndex) {
            OssSplit ossSplit = splits.get(splitIndex);
            if (!ossSplit.isInit()) {
                synchronized (lock) {
                    ossSplit.init(ossTableScan, context, searchArgument, columns);
                }
            }
            return ossSplit.getReadOptions();
        }

        public void initialSearchArgs() {
            if (ossTableScan.getOrcNode().getFilters().isEmpty()) {
                buildSearchArgumentAndColumns(null);
            } else {
                buildSearchArgumentAndColumns(ossTableScan.getOrcNode().getFilters().get(0));
            }
        }

        public synchronized void initWaitFuture(ListenableFuture<List<BloomFilterInfo>> listListenableFuture) {
            if (waitBloomFilterFuture == null) {
                waitBloomFilterFuture = SettableFuture.create();

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

                needWaitBloomFilter = true;
            }
        }

        private void registerBloomFilter(List<BloomFilterInfo> bfInfos) {
            try {
                bloomFilterInfos = bfInfos.stream()
                    .collect(Collectors.toMap(BloomFilterInfo::getId, Function.identity(), (info1, info2) -> info1));

                RexBuilder rexBuilder = ossTableScan.getCluster().getRexBuilder();

                Map<Integer, RexCall> bloomFiltersMap = ossTableScan.getBloomFiltersMap();

                RexNode bloomFilterCondition;
                if (bloomFiltersMap.size() == 1) {
                    bloomFilterCondition = bloomFiltersMap.values().iterator().next();
                } else {
                    bloomFilterCondition = rexBuilder
                        .makeCall(TddlOperatorTable.AND, bloomFiltersMap.values().stream().collect(Collectors.toList()));
                }

                if (ossTableScan.getOrcNode().getFilters().isEmpty()) {
                    buildSearchArgumentAndColumns(bloomFilterCondition);
                } else {
                    buildSearchArgumentAndColumns(rexBuilder
                        .makeCall(TddlOperatorTable.AND, ossTableScan.getOrcNode().getFilters().get(0),
                            bloomFilterCondition));
                }

            } catch (Throwable t) {
                throw new TddlNestableRuntimeException(t);
            }
        }

        private void buildSearchArgumentAndColumns(RexNode rexNode) {
            // init searchArgument and columns
            if (rexNode == null) {
                // full scan
                searchArgument = SearchArgumentFactory
                    .newBuilder()
                    .literal(SearchArgument.TruthValue.YES_NO)
                    .build();
                columns = null;
            } else {
                Parameters parameters = context.getParams();
                OSSPredicateBuilder predicateBuilder =
                    new OSSPredicateBuilder(parameters, ossTableScan.getOrcNode().getInputProjectRowType().getFieldList(),
                        bloomFilterInfos, ossTableScan.getOrcNode().getRowType().getFieldList(),
                        CBOUtil.getTableMeta(ossTableScan.getTable()), sessionProperties);
                Boolean valid = rexNode.accept(predicateBuilder);
                if (valid != null && valid.booleanValue()) {
                    searchArgument = predicateBuilder.build();
                    columns = predicateBuilder.columns();
                } else {
                    // full scan
                    searchArgument = SearchArgumentFactory
                        .newBuilder()
                        .literal(SearchArgument.TruthValue.YES_NO)
                        .build();
                    columns = null;
                }
            }
        }
    }

    /**
     * a of results from
     */
    public class ResultFromOSS {
        private Chunk chunk;
        private VectorizedRowBatch batch;

        public ResultFromOSS(Chunk chunk) {
            this.chunk = chunk;
            this.batch = null;
        }

        public ResultFromOSS(VectorizedRowBatch batch) {
            this.chunk = null;
            this.batch = batch;
        }

        public VectorizedRowBatch getBatch() {
            return batch;
        }

        public Chunk getChunk() {
            return chunk;
        }

        public boolean isChunk() {
            return chunk != null;
        }

        boolean shouldRecycle() {
            return batch != null;
        }
    }
}
