package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.mpp.spi.ConnectorSplit;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutorService;

public interface ColumnarSplit extends ConnectorSplit, Comparable<ColumnarSplit> {

    /**
     * The unique identifier of the split.
     */
    int getSequenceId();

    /**
     * The unique identifier of the columnar data file.
     */
    int getFileId();

    /**
     * Get the next executable scan work.
     * It must record the inner states including the last IO position.
     *
     * @param <SplitT> class of split.
     * @param <BATCH> class of data batch.
     * @return the next executable scan work.
     */
    <SplitT extends ColumnarSplit, BATCH> ScanWork<SplitT, BATCH> nextWork();

    @Deprecated
    @Override
    default Object getInfo() {
        return null;
    }

    ColumnarSplitPriority getPriority();

    @Override
    default int compareTo(ColumnarSplit split) {
        return Integer.compare(getPriority().getValue(), split.getPriority().getValue());
    }

    interface ColumnarSplitBuilder {
        ColumnarSplit build();

        ColumnarSplitBuilder executionContext(ExecutionContext context);

        ColumnarSplitBuilder ioExecutor(ExecutorService ioExecutor);

        ColumnarSplitBuilder fileSystem(FileSystem fileSystem, Engine engine);

        ColumnarSplitBuilder configuration(Configuration configuration);

        ColumnarSplitBuilder sequenceId(int sequenceId);

        ColumnarSplitBuilder file(Path filePath, int fileId);

        ColumnarSplitBuilder tableMeta(String logicalSchema, String logicalTable);

        ColumnarSplitBuilder columnTransformer(OSSColumnTransformer ossColumnTransformer);

        ColumnarSplitBuilder inputRefs(List<Integer> inputRefsForFilter, List<Integer> inputRefsForProject);

        ColumnarSplitBuilder cacheManager(BlockCacheManager<Block> blockCacheManager);

        ColumnarSplitBuilder chunkLimit(int chunkLimit);

        ColumnarSplitBuilder morselUnit(int rgThreshold);

        ColumnarSplitBuilder pushDown(LazyEvaluator<Chunk, BitSet> lazyEvaluator);

        ColumnarSplitBuilder prepare(ScanPreProcessor scanPreProcessor);

        ColumnarSplitBuilder columnarManager(ColumnarManager columnarManager);

        ColumnarSplitBuilder isColumnarMode(boolean isColumnarMode);

        ColumnarSplitBuilder tso(Long tso);

        ColumnarSplitBuilder partNum(int partNum);

        ColumnarSplitBuilder nodePartCount(int nodePartCount);

        ColumnarSplitBuilder memoryAllocator(MemoryAllocatorCtx memoryAllocatorCtx);

        ColumnarSplitBuilder fragmentRFManager(FragmentRFManager fragmentRFManager);

        ColumnarSplitBuilder operatorStatistic(OperatorStatistics operatorStatistics);
    }

    public enum ColumnarSplitPriority {
        /**
         * orc file has lower priority
         */
        ORC_SPLIT_PRIORITY(1),
        /**
         * csv file has the highest priority, should be read in advanced
         */
        CSV_SPLIT_PRIORITY(0);

        /**
         * small number of priority means higher priority
         */
        private final int priority;

        ColumnarSplitPriority(int priority) {
            this.priority = priority;
        }

        public int getValue() {
            return priority;
        }
    }
}
