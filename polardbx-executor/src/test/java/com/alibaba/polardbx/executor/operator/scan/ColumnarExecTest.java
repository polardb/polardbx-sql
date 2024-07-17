package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.scan.impl.MockScanPreProcessor;
import com.alibaba.polardbx.executor.operator.scan.impl.MorselColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.impl.SimpleWorkPool;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class ColumnarExecTest extends ScanTestBase {
    private static final ExecutorService IO_EXECUTOR = Executors.newFixedThreadPool(1,
        new NamedThreadFactory("columnar-io"));

    private static final ExecutorService SCAN_EXECUTOR = Executors.newFixedThreadPool(1,
        new NamedThreadFactory("columnar-scan"));

    private static final int DEFAULT_MORSEL_UNIT = 5;

    private static final double DEFAULT_GROUPS_RATIO = 1D;
    private static final double DEFAULT_DELETION_RATIO = 0D;

    @Test
    public void testMockDriver() throws Throwable {
        MockColumnarScanExec exec = new MockColumnarScanExec();
        exec.open();

        long lastPk = -1L;
        while (!exec.isFinished()) {
            Chunk chunk = exec.nextChunk();

            if (chunk == null) {
                System.out.println("blocked!");
                ListenableFuture<?> future = exec.isBlocked();
                future.get();
                System.out.println("notified!");
                continue;
            }

            // load all
            System.out.println("load chunk");
            for (int i = 0; i < chunk.getBlockCount(); i++) {
                Block block = chunk.getBlock(i);
                block.isNull(0);
                // it will invoke loading.
                if (i == 0) {
                    if (lastPk == -1L) {
                        lastPk = (long) block.getObject(0);
                    } else {
                        long pk = (long) block.getObject(0);
                        Assert.assertTrue(pk - lastPk == 1000L, "last pk = " + lastPk + ", pk = " + pk);
                        lastPk = pk;
                    }
                    System.out.println("start pk = " + block.getObject(0));
                }
            }
        }

        System.out.println("finished");

    }

    private class MockColumnarScanExec {
        // manage columnar-split and scan-works.
        private WorkPool<ColumnarSplit, Chunk> workPool;

        // manage scan-works.
        private volatile ScanWork<ColumnarSplit, Chunk> currentWork;
        private volatile boolean lastWorkNotExecutable;
        private volatile boolean noAvailableWork;
        private Map<String, ScanWork<ColumnarSplit, Chunk>> finishedWorks;

        // manage io-status.
        private volatile IOStatus<Chunk> currentIOStatus;
        private volatile boolean lastStatusRunOut;

        private ScanPreProcessor preProcessor;
        Supplier<ListenableFuture<?>> blockedSupplier;
        private ListenableFuture preProcessorFuture;

        MockColumnarScanExec() {
            this.workPool = new SimpleWorkPool();
            this.lastWorkNotExecutable = true;
            this.lastStatusRunOut = true;
            this.finishedWorks = new TreeMap<>(String::compareTo);

            Engine engine = Engine.LOCAL_DISK;
            FileSystem fileSystem = FILESYSTEM;
            Configuration configuration = CONFIGURATION;

            // build pre-processor for splits to contain all time-consuming processing
            // like pruning, bitmap loading and metadata preheating.
            if (preProcessor == null) {
                preProcessor = new MockScanPreProcessor(
                    configuration, fileSystem, null, null,
                    DEFAULT_GROUPS_RATIO,
                    DEFAULT_DELETION_RATIO,
                    false);

                preProcessor.addFile(FILE_PATH);
            }

            // Schema-level cache manager.
            BlockCacheManager<Block> blockCacheManager = BlockCacheManager.getInstance();

            // Get the push-down predicate.
            // The refs of input-type will be consistent with refs in RexNode.
            LazyEvaluator<Chunk, BitSet> evaluator = null;
            List<Integer> inputRefsForFilter = ImmutableList.of();

            List<Integer> inputRefsForProject = ImmutableList.of(0, 1, 2, 3);

            // The pre-processor shared by all columnar-splits in this table scan.
            Path filePath = FILE_PATH;

            // todo need columnar file-id mapping.
            int fileId = 0;
            final int chunkLimit = DEFAULT_CHUNK_LIMIT;
            final int morselUnit = DEFAULT_MORSEL_UNIT;

            ColumnarSplit columnarSplit = MorselColumnarSplit.newBuilder()
                .executionContext(new ExecutionContext())
                .ioExecutor(IO_EXECUTOR)
                .fileSystem(fileSystem, engine)
                .configuration(configuration)
                .sequenceId(SEQUENCE_ID)
                .file(filePath, fileId)
                .columnTransformer(new OSSColumnTransformer(COLUMN_METAS, COLUMN_METAS, null, null, LOC_IN_ORC))
                .inputRefs(inputRefsForFilter, inputRefsForProject)
                .cacheManager(blockCacheManager)
                .chunkLimit(chunkLimit)
                .morselUnit(morselUnit)
                .pushDown(evaluator)
                .prepare(preProcessor)
                .columnarManager(mockColumnarManager)
                .memoryAllocator(memoryAllocatorCtx)
                .build();

            workPool.addSplit(SEQUENCE_ID, columnarSplit);

            workPool.noMoreSplits(SEQUENCE_ID);
        }

        void open() {
            // invoke pre-processor.
            if (preProcessor != null) {
                preProcessorFuture = preProcessor.prepare(SCAN_EXECUTOR, null, null);
            }
        }

        Chunk nextChunk() {
            // Firstly, Check if pre-processor is done.
            if (preProcessorFuture != null && !preProcessorFuture.isDone()) {

                // The blocked future is from pre-processor.
                blockedSupplier = () -> preProcessorFuture;
                return null;
            } else {

                // The blocked future is from IOStatus.
                blockedSupplier = () -> {
                    System.out.println("wait for " + currentIOStatus.workId());
                    return currentIOStatus.isBlocked();
                };
            }

            tryInvokeNext();

            // fetch the next chunk according to the state.
            IOStatus<Chunk> ioStatus = currentIOStatus;
            ScanState state = ioStatus.state();
            Chunk result;
            switch (state) {
            case READY:
            case BLOCKED: {
                result = ioStatus.popResult();
                // if chunk is null, the Driver should call is_blocked
                return result;
            }
            case FINISHED: {
                // We must firstly mark the last work to state of not-executable,
                // so that when fetch the next chunks from the last IOStatus, The Exec can
                // invoke the next work.
                // System.out.println("check io-status workId = " + ioStatus.workId());
                if (currentWork != null && currentWork.getWorkId().equals(ioStatus.workId())) {
                    lastWorkNotExecutable = true;
                }

                // Try to pop all the rest results
                while ((result = ioStatus.popResult()) != null) {
                    return result;
                }

                // The results of this scan work is run out.
                System.out.println("put current-work workId = " + currentWork.getWorkId());
                finishedWorks.put(currentWork.getWorkId(), currentWork);
                lastStatusRunOut = true;

                tryInvokeNext();

                break;
            }
            case FAILED: {
                if (currentWork != null && currentWork.getWorkId().equals(ioStatus.workId())) {
                    lastWorkNotExecutable = true;
                }
                // throw any stored exception in client.
                ioStatus.throwIfFailed();
                break;
            }
            case CLOSED: {
                if (currentWork != null && currentWork.getWorkId().equals(ioStatus.workId())) {
                    lastWorkNotExecutable = true;
                }
                // The results of this scan work is run out.
                finishedWorks.put(currentWork.getWorkId(), currentWork);
                lastStatusRunOut = true;

                tryInvokeNext();

                break;
            }
            }

            return null;
        }

        void tryInvokeNext() {
            // if the current scan work is no longer executable?
            if (lastWorkNotExecutable) {
                // should recycle the resources of the last scan work.
                // pick up the next split from work pool.
                ScanWork<ColumnarSplit, Chunk> newWork = workPool.pickUp(SEQUENCE_ID);
                if (newWork == null) {
                    noAvailableWork = true;
                } else {
                    currentWork = newWork;
                    currentWork.invoke(SCAN_EXECUTOR);

                    lastWorkNotExecutable = false;
                }
            }

            // if the current io status is run out?
            if (lastStatusRunOut && !noAvailableWork) {
                currentIOStatus = currentWork.getIOStatus();
                lastStatusRunOut = false;
                System.out.println("current io-status = " + currentWork.getWorkId());
            }
        }

        public boolean isFinished() {
            return noAvailableWork && lastWorkNotExecutable && lastStatusRunOut;
        }

        public ListenableFuture<?> isBlocked() {
            return blockedSupplier.get();
        }
    }
}
