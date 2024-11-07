package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.AbstractBlock;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.CacheReader;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.LogicalRowGroup;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.codahale.metrics.Counter;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcProto;

import java.io.IOException;

/**
 * This block loader never uses block cache.
 */
public class NoCacheBlockLoader extends ReactiveBlockLoader {
    public NoCacheBlockLoader(
        LogicalRowGroup<Block, ColumnStatistics> logicalRowGroup,
        int columnId, int startPosition, int positionCount, OrcProto.ColumnEncoding encoding,
        ColumnReader columnReader,
        CacheReader<Block> cacheReader,
        BlockCacheManager<Block> blockCacheManager,
        ExecutionContext context, boolean useBlockCache,
        boolean enableColumnReaderLock, int chunkLimit, Counter loadTimer,
        Counter memoryCounter, boolean onlyCachePrimaryKey, boolean enableSkipCompression) {
        super(logicalRowGroup, columnId, startPosition, positionCount, encoding, columnReader, cacheReader,
            blockCacheManager, context, useBlockCache, enableColumnReaderLock, chunkLimit, loadTimer, memoryCounter,
            onlyCachePrimaryKey, enableSkipCompression);
    }

    @Override
    public Block load(DataType dataType, int[] selection, int selSize) throws IOException {
        // In this case, we need to proactively open column-reader before this method.
        if (!columnReader.isOpened()) {
            throw GeneralUtil.nestedException("column reader has not already been opened.");
        }

        long start = System.nanoTime();
        Block block = parseBlock(dataType, selection, selSize);
        block.cast(AbstractBlock.class).updateSizeInfo();
        if (memoryCounter != null) {
            memoryCounter.inc(block.estimateSize());
        }

        if (loadTimer != null) {
            loadTimer.inc(System.nanoTime() - start);
        }
        return block;
    }
}
