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

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.AbstractBlock;
import com.alibaba.polardbx.executor.chunk.BigIntegerBlock;
import com.alibaba.polardbx.executor.chunk.BlobBlock;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.ByteArrayBlock;
import com.alibaba.polardbx.executor.chunk.ByteBlock;
import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DoubleBlock;
import com.alibaba.polardbx.executor.chunk.EnumBlock;
import com.alibaba.polardbx.executor.chunk.FloatBlock;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ShortBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.alibaba.polardbx.executor.chunk.TimeBlock;
import com.alibaba.polardbx.executor.chunk.TimestampBlock;
import com.alibaba.polardbx.executor.chunk.ULongBlock;
import com.alibaba.polardbx.executor.chunk.columnar.BlockLoader;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.CacheReader;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.LogicalRowGroup;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BigBitType;
import com.alibaba.polardbx.optimizer.core.datatype.BinaryType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.utils.TimestampUtils;
import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.TimeZone;

/**
 * A reactive block loader depends on IO/cache modules from the outside scheduler.
 * 1. The lifecycle of these modules must be in opened/prepared state for loading.
 * 2. These modules are shared in stripe-level on given column.
 */
public class ReactiveBlockLoader implements BlockLoader {
    protected static final Logger LOGGER = LoggerFactory.getLogger("oss");
    protected final LogicalRowGroup<Block, ColumnStatistics> logicalRowGroup;

    /**
     * Column id on schema.
     */
    protected final int columnId;

    /**
     * Row group id in ORC Stripe.
     */
    protected final int rowGroupId;

    /**
     * The start position at column reader for this block.
     */
    protected final int startPosition;

    /**
     * The position count of this block.
     */
    protected final int positionCount;

    /**
     * Column-level encoding type.
     */
    protected final OrcProto.ColumnEncoding encoding;

    /**
     * A column-level reader responsible for all blocks of all row groups in the stripe.
     */
    protected final ColumnReader columnReader;

    /**
     * A column-level cache reader holding the available cached blocks in the stripe
     */
    private final CacheReader<Block> cacheReader;

    /**
     * The global block cache manager shared by all files.
     */
    private final BlockCacheManager<Block> blockCacheManager;

    private final ExecutionContext context;
    private final boolean useBlockCache;
    protected final boolean enableColumnReaderLock;
    private final int chunkLimit;

    protected final Counter loadTimer;
    protected final Counter memoryCounter;
    private final TimeZone timeZone;

    private final boolean onlyCachePrimaryKey;
    protected final boolean enableSkipCompression;

    protected int lastSkipCount;

    public ReactiveBlockLoader(LogicalRowGroup<Block, ColumnStatistics> logicalRowGroup, int columnId,
                               int startPosition,
                               int positionCount, OrcProto.ColumnEncoding encoding, ColumnReader columnReader,
                               CacheReader<Block> cacheReader,
                               BlockCacheManager<Block> blockCacheManager, ExecutionContext context,
                               boolean useBlockCache, boolean enableColumnReaderLock, int chunkLimit,
                               Counter loadTimer, Counter memoryCounter, boolean onlyCachePrimaryKey,
                               boolean enableSkipCompression) {
        this.logicalRowGroup = logicalRowGroup;
        this.columnId = columnId;
        this.rowGroupId = logicalRowGroup.groupId();
        this.startPosition = startPosition;
        this.positionCount = positionCount;
        this.encoding = encoding;
        this.columnReader = columnReader;
        this.cacheReader = cacheReader;
        this.blockCacheManager = blockCacheManager;
        this.context = context;
        this.useBlockCache = useBlockCache;
        this.enableColumnReaderLock = enableColumnReaderLock;
        this.chunkLimit = chunkLimit;
        this.loadTimer = loadTimer;
        this.memoryCounter = memoryCounter;
        this.timeZone = TimestampUtils.getTimeZone(context);

        this.onlyCachePrimaryKey = onlyCachePrimaryKey;
        this.enableSkipCompression = enableSkipCompression;
    }

    @Override
    public Block load(DataType dataType, int[] selection, int selSize) throws IOException {
        // In this case, we need to proactively open column-reader before this method.
        if (!columnReader.isOpened()) {
            throw GeneralUtil.nestedException("column reader has not already been opened.");
        }

        if (!cacheReader.isInitialized()) {
            throw GeneralUtil.nestedException("cache reader has not already been initialized.");
        }

        long start = System.nanoTime();
        Block cached;
        if ((cached = cacheReader.getCache(rowGroupId, startPosition)) != null) {
            // If cache hit
            // zero copy?
            // return LazyBlockUtils.copy(cached);
            if (loadTimer != null) {
                loadTimer.inc(System.nanoTime() - start);
            }
            return cached;
        } else {
            // cache miss, decoding from raw bytes.
            Block block = parseBlock(dataType, selection, selSize);
            block.cast(AbstractBlock.class).updateSizeInfo();
            if (memoryCounter != null) {
                memoryCounter.inc(block.estimateSize());
            }

            // write back to cache manager
            // condition1: use block cache
            // condition2: there is no compression block has been skipped.
            // condition3: the column is primary key.
            if (useBlockCache
                && (!enableSkipCompression || lastSkipCount == 0)
                && (!onlyCachePrimaryKey || columnReader.needCache())) {
                blockCacheManager.putCache(
                    block, // cache entity
                    chunkLimit,
                    logicalRowGroup.rowCount(), // for boundary check
                    logicalRowGroup.path(), logicalRowGroup.stripeId(), logicalRowGroup.groupId(), // base info
                    columnId, startPosition, positionCount // location of block in row-group
                );
            }
            if (loadTimer != null) {
                loadTimer.inc(System.nanoTime() - start);
            }
            return block;
        }
    }

    private Block parseBlock(DataType dataType, int[] selection, int selSize) throws IOException {
        boolean compatible = context.isEnableOssCompatible();
        Block targetBlock = allocateBlock(dataType, compatible, timeZone);

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

    private Block allocateBlock(DataType inputType, boolean compatible, TimeZone timeZone) {
        // NOTE: we need more type-specified implementations
        switch (inputType.fieldType()) {
        case MYSQL_TYPE_LONGLONG: {
            if (!inputType.isUnsigned()) {
                Preconditions.checkArgument(columnReader instanceof LongColumnReader);
                // for signed bigint.
                return new LongBlock(inputType, positionCount);
            } else {
                Preconditions.checkArgument(columnReader instanceof UnsignedLongColumnReader);
                // for unsigned bigint
                return new ULongBlock(inputType, positionCount);
            }
        }
        case MYSQL_TYPE_LONG: {
            if (!inputType.isUnsigned()) {
                Preconditions.checkArgument(columnReader instanceof IntegerColumnReader);
                // for signed int
                return new IntegerBlock(inputType, positionCount);
            } else {
                Preconditions.checkArgument(columnReader instanceof LongColumnReader);
                // for unsigned int
                return new LongBlock(inputType, positionCount);
            }
        }
        case MYSQL_TYPE_SHORT: {
            if (!inputType.isUnsigned()) {
                Preconditions.checkArgument(columnReader instanceof ShortColumnReader);
                // for signed short
                return new ShortBlock(inputType, positionCount);
            } else {
                Preconditions.checkArgument(columnReader instanceof IntegerColumnReader);
                // for unsigned short
                return new IntegerBlock(inputType, positionCount);
            }
        }
        case MYSQL_TYPE_INT24: {
            Preconditions.checkArgument(columnReader instanceof IntegerColumnReader);
            return new IntegerBlock(inputType, positionCount);
        }
        case MYSQL_TYPE_TINY: {
            if (!inputType.isUnsigned()) {
                Preconditions.checkArgument(columnReader instanceof ByteColumnReader);
                // for signed tiny
                return new ByteBlock(inputType, positionCount);
            } else {
                Preconditions.checkArgument(columnReader instanceof ShortColumnReader);
                // for unsigned tiny
                return new ShortBlock(inputType, positionCount);
            }
        }
        case MYSQL_TYPE_FLOAT: {
            Preconditions.checkArgument(columnReader instanceof FloatColumnReader);
            return new FloatBlock(inputType, positionCount);
        }
        case MYSQL_TYPE_DOUBLE: {
            Preconditions.checkArgument(columnReader instanceof DoubleColumnReader);
            return new DoubleBlock(inputType, positionCount);
        }
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2: {
            Preconditions.checkArgument(columnReader instanceof TimestampColumnReader);
            return new TimestampBlock(inputType, positionCount, timeZone);
        }
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2: {
            Preconditions.checkArgument(columnReader instanceof LongColumnReader);
            return new TimestampBlock(inputType, positionCount, timeZone);
        }
        case MYSQL_TYPE_YEAR: {
            Preconditions.checkArgument(columnReader instanceof LongColumnReader);
            return new LongBlock(inputType, positionCount);
        }
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE: {
            Preconditions.checkArgument(columnReader instanceof PackedTimeColumnReader);
            return new DateBlock(positionCount, timeZone);
        }
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2: {
            Preconditions.checkArgument(columnReader instanceof PackedTimeColumnReader);
            // for date
            return new TimeBlock(inputType, positionCount, timeZone);
        }
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL: {
            if (columnReader instanceof LongColumnReader) {
                return new DecimalBlock(inputType, positionCount, true);
            } else {
                return new DecimalBlock(inputType, positionCount, false);
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
                    Preconditions.checkArgument(columnReader instanceof DirectVarcharColumnReader);
                    // For column in direct encoding
                    return new SliceBlock((SliceType) inputType, positionCount, compatible, false);
                }
                case DICTIONARY:
                case DICTIONARY_V2:
                    boolean enableSliceDict =
                        context.getParamManager().getBoolean(ConnectionParams.ENABLE_COLUMNAR_SLICE_DICT);

                    Preconditions.checkArgument(columnReader instanceof DictionaryVarcharColumnReader);
                    // for dictionary encoding
                    return new SliceBlock((SliceType) inputType, positionCount, compatible, enableSliceDict);
                default:
                    throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
                }
            } else {
                throw new UnsupportedOperationException("String type not supported: " + inputType.getClass().getName());
            }
        }
        case MYSQL_TYPE_ENUM: {
            switch (encoding.getKind()) {
            case DIRECT:
            case DIRECT_V2: {
                Preconditions.checkArgument(columnReader instanceof DirectEnumColumnReader);
                // For column in direct encoding
                return new EnumBlock(positionCount, ((EnumType) inputType).getEnumValues());
            }
            case DICTIONARY:
            case DICTIONARY_V2:
                Preconditions.checkArgument(columnReader instanceof DictionaryEnumColumnReader);
                // for dictionary encoding
                return new EnumBlock(positionCount, ((EnumType) inputType).getEnumValues());
            default:
                throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
            }
        }
        case MYSQL_TYPE_JSON: {
            switch (encoding.getKind()) {
            case DIRECT:
            case DIRECT_V2:
                Preconditions.checkArgument(columnReader instanceof DirectJsonColumnReader);
                return new StringBlock(inputType, positionCount);
            case DICTIONARY:
            case DICTIONARY_V2:
                Preconditions.checkArgument(columnReader instanceof DictionaryJsonColumnReader);
                return new StringBlock(inputType, positionCount);
            default:
                throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
            }
        }
        case MYSQL_TYPE_BIT: {
            if (inputType instanceof BigBitType) {
                Preconditions.checkArgument(columnReader instanceof BigBitColumnReader);
                return new BigIntegerBlock(positionCount);
            } else {
                Preconditions.checkArgument(columnReader instanceof IntegerColumnReader);
                return new IntegerBlock(inputType, positionCount);
            }
        }
        case MYSQL_TYPE_BLOB: {
            // For column in both direct encoding and dictionary encoding
            return new BlobBlock(positionCount);
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
        return this.cacheReader;
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
        return "ReactiveBlockLoader{" +
            "logicalRowGroup=" + logicalRowGroup +
            ", columnId=" + columnId +
            ", rowGroupId=" + rowGroupId +
            ", startPosition=" + startPosition +
            ", positionCount=" + positionCount +
            '}';
    }
}
