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
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.executor.archive.reader.ORCReadResult;
import com.alibaba.polardbx.executor.archive.reader.ORCReaderWithAggTask;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.archive.reader.OSSReadOption;
import com.alibaba.polardbx.executor.archive.reader.UnPushableORCReaderTask;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.ColumnarStoreUtils;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.UserMetadataUtil;
import org.apache.orc.impl.OrcTail;

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
    public static final int TIMEOUT = 8000000;
    private static final int POOL_SIZE = 2;
    private final int chunkLimit;
    private final List<SettableFuture<?>> blockedCallers = new ArrayList<>();
    protected TddlRuntimeException exception = null;
    private OSSTableClientInitializer initializer;
    private List<OssSplit> splits;
    private int splitIndex;
    private Map<Integer, Pool<VectorizedRowBatch>> poolMap;
    private ConcurrentLinkedQueue<ResultFromOSS> results;
    private ExecutionContext context;
    private SessionProperties sessionProperties;
    private volatile SettableFuture<?> waitBloomFilterFuture = null;
    private volatile boolean needWaitBloomFilter;
    private boolean isFinished;
    private boolean isClosed;
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

        this.poolMap = Maps.newTreeMap();
        this.results = new ConcurrentLinkedQueue<>();

        this.isFinished = false;
        this.isClosed = false;

        this.initializer = new OSSTableClientInitializer(ossTableScan);
        this.initializer.prepareSearchArgs();
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

        for (int i = 0; i < splits.size(); i++) {
            this.poolMap.put(i, new Pool<>(POOL_SIZE, TIMEOUT));
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
        poolMap.get(result.getPoolIndex()).recycle(result.getBatch());
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

    public synchronized void setIsFinish() {
        this.isFinished = true;
        notifyBlockedCallers();
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

    private TypeDescription.RowBatchVersion getRowBatchVersion(FileMeta fileMeta) {
        if (fileMeta instanceof OSSOrcFileMeta) {
            return ((OSSOrcFileMeta) fileMeta).isEnableDecimal64() ? TypeDescription.RowBatchVersion.USE_DECIMAL64 :
                TypeDescription.RowBatchVersion.ORIGINAL;
        }
        return TypeDescription.RowBatchVersion.ORIGINAL;
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

                    // for each split, check its delta read option firstly.
                    OssSplit split = splits.get(splitIndex);
                    OssSplit.DeltaReadOption deltaReadOption;
                    if ((deltaReadOption = split.getDeltaReadOption()) != null) {
                        // It must be in columnar mode when delta read option is not null.
                        ColumnarManager columnarManager = ColumnarManager.getInstance();

                        final long checkpointTso = deltaReadOption.getCheckpointTso();
                        final Map<String, List<String>> allCsvFiles = deltaReadOption.getAllCsvFiles();
                        final List<Integer> projectColumnIndexes = deltaReadOption.getProjectColumnIndexes();

                        allCsvFiles.values().stream().flatMap(List::stream).forEach(
                            csvFile -> foreachDeltaFile(csvFile, checkpointTso, columnarManager,
                                projectColumnIndexes)
                        );
                    }

                    Long checkpointTso = splits.get(splitIndex).getCheckpointTso();
                    for (OSSReadOption readOption : readOptions) {
                        for (int i = 0; i < readOption.getTableFileList().size(); i++) {
                            FileMeta fileMeta = readOption.getPhyTableFileMetas().get(i);
                            TypeDescription.RowBatchVersion rowBatchVersion = getRowBatchVersion(fileMeta);

                            // supply initial elements to pool.
                            poolMap.get(splitIndex)
                                .supply(() -> readOption.getReadSchema().createRowBatch(rowBatchVersion, chunkLimit));
                            // do fetch file rows.
                            foreachFile(readOption, i, splitIndex, checkpointTso);
                        }
                    }
                }
                // If physical table list or file list is empty, notify the blocked thread there.
                setIsFinish();
            } catch (Throwable t) {
                setException(new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, t.getMessage(), t));
            }

            // whatever
            return null;
        }

        private void foreachDeltaFile(String csvFile, long tso, ColumnarManager columnarManager,
                                      List<Integer> projectColumnIndexes) {
            List<Chunk> chunkList = columnarManager.csvData(tso, csvFile);
            int chunkIndex = 0;
            while (!isCancelled) {
                try {
                    if (chunkIndex < chunkList.size()) {
                        Chunk chunk = chunkList.get(chunkIndex);

                        // fill selection array in columnar store mode.
                        int[] selection = new int[chunk.getPositionCount()];
                        IntegerBlock integerBlock =
                            chunk.getBlock(ColumnarStoreUtils.POSITION_COLUMN_INDEX).cast(
                                IntegerBlock.class);
                        int selSize = columnarManager.fillSelection(csvFile, tso, selection, integerBlock);

                        // project columns at given index.
                        Block[] projectBlocks = projectColumnIndexes.stream()
                            .map(chunk::getBlock).collect(Collectors.toList()).toArray(new Block[0]);
                        Chunk result = new Chunk(projectBlocks);

                        ResultFromOSS resultFromOSS = new ResultFromOSS(result, true);
                        resultFromOSS.setSelSize(selSize);
                        resultFromOSS.setSelection(selection);
                        results.add(resultFromOSS);

                        chunkIndex++;
                        notifyBlockedCallers();
                    } else {
                        // no more chunks
                        return;
                    }
                } catch (Throwable t) {
                    setException(new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, t, t.getMessage()));
                }
            }
        }

        private void foreachFile(OSSReadOption readOption, int fileIndex, int poolIndex, Long checkpointTso) {
            String tableFile = readOption.getTableFileList().get(fileIndex);
            FileMeta fileMeta = readOption.getPhyTableFileMetas().get(fileIndex);
            PruningResult pruningResult = readOption.getPruningResultList().get(fileIndex);
            final String fileName = fileMeta.getFileName();

            // build orc reader task for each file.
            UnPushableORCReaderTask task =
                aggCalls == null ?
                    new UnPushableORCReaderTask(readOption, tableFile, fileMeta, pruningResult, context) :
                    new ORCReaderWithAggTask(readOption, tableFile, fileMeta, pruningResult, context,
                        dataTypeList, aggCalls, aggColumns);
            task.init();

            // register for resource management.
            registeredOrcTasks.add(task);

            // fetch all rows from one file.
            while (!isCancelled) {
                VectorizedRowBatch batch = null;
                boolean needRecycle = true;
                try {
                    batch = poolMap.get(poolIndex).poll();
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
                        results.add(new ResultFromOSS(readResult.getChunk(), false));
                    } else {
                        // Fill the result and notify the block callers.
                        // Do not recycle the batch.
                        needRecycle = false;
                        ResultFromOSS resultFromOSS = new ResultFromOSS(batch,
                            task.getOssReadOption().getOssColumnTransformer(),
                            poolIndex);
                        if (readOption.isColumnarIndex()) {
                            // fill selection array in columnar store mode.
                            ColumnarManager columnarManager = ColumnarManager.getInstance();
                            int[] selection = new int[batch.size];

                            // in columnar mode, we set implicit column in first column index.
                            LongColumnVector longColumnVector = (LongColumnVector) batch.cols[0];
                            int selSize =
                                columnarManager.fillSelection(fileName, checkpointTso, selection, longColumnVector,
                                    batch.size);

                            resultFromOSS.setSelSize(selSize);
                            resultFromOSS.setSelection(selection);
                        }
                        results.add(resultFromOSS);
                    }
                    notifyBlockedCallers();
                } catch (Throwable t) {
                    setException(new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, t, t.getMessage()));
                } finally {
                    if (batch != null && needRecycle) {
                        try {
                            poolMap.get(poolIndex).recycle(batch);
                        } catch (InterruptedException e) {
                            setException(new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, e, e.getMessage()));
                        }
                    }
                }
            }
        }
    }

    private class OSSTableClientInitializer {
        private final Object lock = new Object();
        private OSSTableScan ossTableScan;
        private volatile Map<Integer, BloomFilterInfo> bloomFilterInfos;
        private RexNode bloomFilterCondition;
        private volatile ScheduledFuture<?> monitorWaitBloomFilterFuture;

        OSSTableClientInitializer(OSSTableScan ossTableScan) {
            this.ossTableScan = ossTableScan;
        }

        public List<OSSReadOption> lazyInitSplit(int splitIndex) {
            OssSplit ossSplit = splits.get(splitIndex);
            if (!ossSplit.isInit()) {
                synchronized (lock) {
                    ossSplit.init(ossTableScan, context, sessionProperties, bloomFilterInfos, bloomFilterCondition);
                }
            }
            return ossSplit.getReadOptions();
        }

        /**
         * record all information needed for SearchArgs build in ossSplit
         */
        public void prepareSearchArgs() {
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

                if (bloomFiltersMap.size() == 1) {
                    bloomFilterCondition = bloomFiltersMap.values().iterator().next();
                } else {
                    bloomFilterCondition = rexBuilder
                        .makeCall(TddlOperatorTable.AND,
                            bloomFiltersMap.values().stream().collect(Collectors.toList()));
                }

            } catch (Throwable t) {
                throw new TddlNestableRuntimeException(t);
            }
        }
    }

    /**
     * a union of result whether from orcfile or statistics
     */
    public class ResultFromOSS {
        private Chunk chunk;
        private VectorizedRowBatch batch;

        private boolean isDelta;
        private int[] selection;
        private int selSize;
        private int poolIndex;
        private OSSColumnTransformer ossColumnTransformer;

        public ResultFromOSS(Chunk chunk, boolean isDelta) {
            this.chunk = chunk;
            this.batch = null;
            this.isDelta = isDelta;
        }

        public ResultFromOSS(VectorizedRowBatch batch,
                             OSSColumnTransformer ossColumnTransformer,
                             int poolIndex) {
            this.chunk = null;
            this.batch = batch;
            this.isDelta = false;
            this.ossColumnTransformer = ossColumnTransformer;
            this.poolIndex = poolIndex;
        }

        public int getPoolIndex() {
            return poolIndex;
        }

        public int[] getSelection() {
            return selection;
        }

        public void setSelection(int[] selection) {
            this.selection = selection;
        }

        public int getSelSize() {
            return selSize;
        }

        public void setSelSize(int selSize) {
            this.selSize = selSize;
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

        public OSSColumnTransformer getOssColumnTransformer() {
            return ossColumnTransformer;
        }

        boolean shouldRecycle() {
            return batch != null;
        }

        public boolean isDelta() {
            return isDelta;
        }
    }
}
