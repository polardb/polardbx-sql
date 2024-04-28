package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.AbstractBlock;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.ByteArrayBlock;
import com.alibaba.polardbx.executor.chunk.DoubleBlock;
import com.alibaba.polardbx.executor.chunk.FloatBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.columnar.BlockLoader;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.CacheReader;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.LogicalRowGroup;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BinaryType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;

/**
 * This block loader only loads the following raw orc data types from orc data:
 * long, float, double, byte[]
 * And block loaded by this loader MUST NOT be put in block cache.
 */
public class OrcRawTypeBlockLoader extends ReactiveBlockLoader {

    public OrcRawTypeBlockLoader(LogicalRowGroup<Block, ColumnStatistics> logicalRowGroup,
                                 int columnId,
                                 int startPosition,
                                 int positionCount,
                                 OrcProto.ColumnEncoding encoding,
                                 ColumnReader columnReader,
                                 CacheReader<Block> cacheReader,
                                 BlockCacheManager<Block> blockCacheManager,
                                 ExecutionContext context,
                                 boolean useBlockCache,
                                 boolean enableColumnReaderLock,
                                 int chunkLimit,
                                 Counter loadTimer,
                                 Counter memoryCounter,
                                 boolean onlyCachePrimaryKey,
                                 boolean enableSkipCompression) {
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

    private Block parseBlock(DataType dataType, int[] selection, int selSize) throws IOException {
        Block targetBlock = allocateBlock(dataType);

        // Start decoding from given position.
        // The seeking may be blocked util IO processing completed.
        long stamp = enableColumnReaderLock ? columnReader.getLock().writeLock() : -1L;
        try {
            columnReader.startAt(rowGroupId, startPosition);

            if (enableSkipCompression) {
                lastSkipCount = columnReader.next(targetBlock.cast(RandomAccessBlock.class), positionCount,
                    selection, selSize);
            } else {
                columnReader.next(targetBlock.cast(RandomAccessBlock.class), positionCount);
                lastSkipCount = 0;
            }

        } catch (Throwable t) {

            LOGGER.error(MessageFormat.format(
                "parse block error: {0}",
                this.toString()
            ));

            throw t;
        } finally {
            if (enableColumnReaderLock) {
                columnReader.getLock().unlockWrite(stamp);
            }
        }

        return targetBlock;
    }

    private Block allocateBlock(DataType inputType) {
        switch (inputType.fieldType()) {
        case MYSQL_TYPE_LONGLONG:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_YEAR:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2:
        case MYSQL_TYPE_BIT: {
            Preconditions.checkArgument(columnReader instanceof LongColumnReader);
            return new LongBlock(inputType, positionCount);
        }
        case MYSQL_TYPE_FLOAT: {
            Preconditions.checkArgument(columnReader instanceof DoubleBlockFloatColumnReader);
            return new DoubleBlock(inputType, positionCount);
        }
        case MYSQL_TYPE_DOUBLE: {
            Preconditions.checkArgument(columnReader instanceof DoubleColumnReader);
            return new DoubleBlock(inputType, positionCount);
        }
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL: {
            if (columnReader instanceof LongColumnReader) {
                return new LongBlock(inputType, positionCount);
            } else {
                return new ByteArrayBlock(positionCount);
            }
        }
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_VARCHAR: {
            if (inputType instanceof BinaryType) {
                return new ByteArrayBlock(positionCount);
            } else if (inputType instanceof SliceType) {
                switch (encoding.getKind()) {
                case DIRECT:
                case DIRECT_V2: {
                    Preconditions.checkArgument(columnReader instanceof DirectBinaryColumnReader);
                    // For column in direct encoding
                    return new ByteArrayBlock(positionCount);
                }
                case DICTIONARY:
                case DICTIONARY_V2:
                    Preconditions.checkArgument(columnReader instanceof DictionaryBinaryColumnReader);
                    // for dictionary encoding
                    return new ByteArrayBlock(positionCount);
                default:
                    throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
                }
            } else {
                throw new UnsupportedOperationException("String type not supported: " + inputType.getClass().getName());
            }
        }
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_BLOB: {
            switch (encoding.getKind()) {
            case DIRECT:
            case DIRECT_V2: {
                Preconditions.checkArgument(columnReader instanceof DirectBinaryColumnReader);
                // For column in direct encoding
                return new ByteArrayBlock(positionCount);
            }
            case DICTIONARY:
            case DICTIONARY_V2:
                Preconditions.checkArgument(columnReader instanceof DictionaryBinaryColumnReader);
                // for dictionary encoding
                return new ByteArrayBlock(positionCount);
            default:
                throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
            }
        }
        case MYSQL_TYPE_JSON: {
            Preconditions.checkArgument(columnReader instanceof DirectBinaryColumnReader);
            return new ByteArrayBlock(positionCount);
        }

        default:
        }
        throw GeneralUtil.nestedException("Unsupported input-type " + inputType);
    }

    @Override
    public ColumnReader getColumnReader() {
        return this.columnReader;
    }

    @Override
    public CacheReader<Block> getCacheReader() {
        return null;
    }

    @Override
    public int startPosition() {
        return startPosition;
    }

    @Override
    public int positionCount() {
        return positionCount;
    }

    @Override
    public String toString() {
        return "RawBlockLoader{" +
            "logicalRowGroup=" + logicalRowGroup +
            ", columnId=" + columnId +
            ", rowGroupId=" + rowGroupId +
            ", startPosition=" + startPosition +
            ", positionCount=" + positionCount +
            '}';
    }
}
