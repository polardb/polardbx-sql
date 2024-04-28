package com.alibaba.polardbx.executor.chunk.columnar;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.archive.reader.TypeComparison;
import com.alibaba.polardbx.executor.chunk.AbstractBlock;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockConverter;
import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.CastableBlock;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryMapping;
import com.alibaba.polardbx.executor.operator.util.DriverObjectPool;
import com.alibaba.polardbx.executor.operator.util.TypedList;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.accumulator.state.NullableLongGroupState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.BitSet;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

public class CommonLazyBlock implements LazyBlock {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");
    public static final int REF_QUOTA_NUM = 1;
    private final DataType targetType;

    private final OSSColumnTransformer columnTransformer;
    private final int colId;

    /**
     * Loader for memory allocation and parsing.
     */
    private final BlockLoader blockLoader;

    private final ColumnReader columnReader;

    private final boolean useSelection;
    private final boolean enableCompatible;

    private final AtomicInteger refQuota;
    private TimeZone timeZone;

    /**
     * Internal block implementation.
     */
    private AbstractBlock block;

    /**
     * Has this lazy block been loaded.
     */
    private boolean isLoaded;

    /**
     * Store the exception during loading.
     */
    private Exception loadingException;

    private int[] selection;
    private int selSize;

    private ExecutionContext context;

    public CommonLazyBlock(DataType targetType, BlockLoader blockLoader, ColumnReader columnReader,
                           boolean useSelection, boolean enableCompatible, TimeZone timeZone,
                           ExecutionContext context, int colId, OSSColumnTransformer ossColumnTransformer) {
        this.blockLoader = blockLoader;
        this.targetType = targetType;
        this.columnReader = columnReader;
        this.useSelection = useSelection;
        this.enableCompatible = enableCompatible;
        this.timeZone = timeZone;

        this.refQuota = new AtomicInteger(REF_QUOTA_NUM);

        if (this.columnReader != null) {
            this.columnReader.retainRef(REF_QUOTA_NUM);
        }

        this.isLoaded = false;
        this.block = null;
        this.loadingException = null;
        this.context = context;
        this.colId = colId;
        this.columnTransformer = ossColumnTransformer;
    }

    public void setSelection(int[] selection, int selSize) {
        this.selection = selection;
        this.selSize = selSize;

        // for filter column that has been loaded.
        if (isLoaded) {
            this.block = fillSelection(block);
        }
    }

    @Override
    public Block getLoaded() {
        return block;
    }

    @Override
    public void load() {
        try {
            // Not concurrency-safe.
            if (loadingException != null) {
                throw GeneralUtil.nestedException(loadingException);
            }
            if (!isLoaded) {
                try {
                    // It may invoke parsing or just fetch cached block.
                    // If the types are not identical, the zero copy will be nonsense
                    AbstractBlock result = loadAndTransformDataType();

                    if (result != null && selection != null && selSize >= 0) {
                        // For zero copy
                        this.block = fillSelection(result);
                    } else {
                        this.block = BlockUtils.wrapNullSelection(result, true, enableCompatible, timeZone);
                    }

                } catch (IOException e) {
                    loadingException = e;
                    throw GeneralUtil.nestedException(e);
                }
                isLoaded = true;
            }
        } finally {
            releaseRef();
        }
    }

    private AbstractBlock loadAndTransformDataType() throws IOException {
        ColumnMeta sourceColumnMeta = columnTransformer.getSourceColumnMeta(colId);
        ColumnMeta targetColumnMeta = columnTransformer.getTargetColumnMeta(colId);
        TypeComparison comparison = columnTransformer.getCompareResult(colId);

        switch (comparison) {
        case MISSING_EQUAL:
            return (AbstractBlock) OSSColumnTransformer.fillDefaultValue(
                targetType,
                columnTransformer.getInitColumnMeta(colId),
                columnTransformer.getTimeStamp(colId),
                // TODO(siyun): while using selection, this could be optimized
                blockLoader.positionCount(),
                context
            );
        case MISSING_NO_EQUAL:
            return (AbstractBlock) OSSColumnTransformer.fillDefaultValueAndTransform(
                targetColumnMeta,
                columnTransformer.getInitColumnMeta(colId),
                blockLoader.positionCount(),
                context
            );
        default:
            DataType sourceType = sourceColumnMeta.getDataType();
            AbstractBlock sourceBlock = (AbstractBlock) blockLoader.load(sourceType, selection, selSize);
            BlockConverter converter = Converters.createBlockConverter(sourceType, targetType, context);
            return (AbstractBlock) converter.apply(sourceBlock);
        }
    }

    @Override
    public void releaseRef() {
        if (columnReader == null) {
            return;
        }
        // get ref quota and release ref from column reader.
        int ref = refQuota.getAndDecrement();
        if (ref > 0) {
            columnReader.releaseRef(ref);
        }
        // Check status and ref count of column reader, and try to close it.
        if (columnReader.hasNoMoreBlocks() && columnReader.refCount() <= 0) {
            columnReader.close();
        }
    }

    private AbstractBlock fillSelection(AbstractBlock result) {
        return BlockUtils.fillSelection(result, selection, selSize, useSelection, enableCompatible, timeZone);
    }

    @Override
    public int getPositionCount() {
        // don't load
        return selection != null ? selSize : blockLoader.positionCount();
    }

    @Override
    public long getElementUsedBytes() {
        // todo
        return getPositionCount() * 8;
    }

    @Override
    public long estimateSize() {
        // todo
        return getPositionCount() * 8;
    }

    @Override
    public DataType getType() {
        // don't load
        return targetType;
    }

    @Override
    public boolean hasVector() {
        return block != null;
    }

    @Override
    public BlockLoader getLoader() {
        return blockLoader;
    }

    @Override
    public <T extends CastableBlock> T cast(Class<T> clazz) {
        load();
        if (!clazz.isInstance(getLoaded())) {
            throw GeneralUtil.nestedException(new ClassCastException());
        }
        return (T) getLoaded();
    }

    @Override
    public boolean isInstanceOf(Class clazz) {
        load();
        return clazz.isInstance(getLoaded());
    }

    @Override
    public boolean isNull(int position) {
        load();
        return block.isNull(position);
    }

    @Override
    public byte getByte(int position) {
        load();
        return block.getByte(position);
    }

    @Override
    public short getShort(int position) {
        load();
        return block.getShort(position);
    }

    @Override
    public int getInt(int position) {
        load();
        return block.getInt(position);
    }

    @Override
    public long getLong(int position) {
        load();
        return block.getLong(position);
    }

    @Override
    public double getDouble(int position) {
        load();
        return block.getDouble(position);
    }

    @Override
    public float getFloat(int position) {
        load();
        return block.getFloat(position);
    }

    @Override
    public Timestamp getTimestamp(int position) {
        load();
        return block.getTimestamp(position);
    }

    @Override
    public Date getDate(int position) {
        load();
        return block.getDate(position);
    }

    @Override
    public Time getTime(int position) {
        load();
        return block.getTime(position);
    }

    @Override
    public String getString(int position) {
        load();
        return block.getString(position);
    }

    @Override
    public Decimal getDecimal(int position) {
        load();
        return block.getDecimal(position);
    }

    @Override
    public BigInteger getBigInteger(int position) {
        load();
        return block.getBigInteger(position);
    }

    @Override
    public boolean getBoolean(int position) {
        load();
        return block.getBoolean(position);
    }

    @Override
    public byte[] getByteArray(int position) {
        load();
        return block.getByteArray(position);
    }

    @Override
    public Blob getBlob(int position) {
        load();
        return block.getBlob(position);
    }

    @Override
    public Clob getClob(int position) {
        load();
        return block.getClob(position);
    }

    @Override
    public int hashCode(int position) {
        load();
        return block.hashCode(position);
    }

    @Override
    public long hashCodeUseXxhash(int pos) {
        load();
        return block.hashCodeUseXxhash(pos);
    }

    @Override
    public int checksum(int position) {
        load();
        return block.checksum(position);
    }

    @Override
    public int[] hashCodeVector() {
        load();
        return block.hashCodeVector();
    }

    @Override
    public void hashCodeVector(int[] results, int positionCount) {
        load();
        block.hashCodeVector(results, positionCount);
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        load();
        if (other instanceof CommonLazyBlock) {
            ((CommonLazyBlock) other).load();
            return block.equals(position, ((CommonLazyBlock) other).block, otherPosition);
        }
        return block.equals(position, other, otherPosition);
    }

    @Override
    public boolean mayHaveNull() {
        load();
        return block.mayHaveNull();
    }

    @Override
    public Object getObject(int position) {
        load();
        return block.getObject(position);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        load();
        block.writePositionTo(position, blockBuilder);
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        load();
        block.addToHasher(sink, position);
    }

    @Override
    public Object getObjectForCmp(int position) {
        load();
        return block.getObjectForCmp(position);
    }

    @Override
    public void setIsNull(boolean[] isNull) {
        load();
        block.setIsNull(isNull);
    }

    @Override
    public boolean hasNull() {
        load();
        return block.hasNull();
    }

    @Override
    public void setHasNull(boolean hasNull) {
        load();
        block.setHasNull(hasNull);
    }

    @Override
    public boolean[] nulls() {
        load();
        return block.nulls();
    }

    @Override
    public String getDigest() {
        load();
        return block.getDigest();
    }

    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        load();
        block.copySelected(selectedInUse, sel, size, output);
    }

    @Override
    public void shallowCopyTo(RandomAccessBlock another) {
        load();
        block.shallowCopyTo(another);
    }

    @Override
    public Object elementAt(int position) {
        load();
        return block.elementAt(position);
    }

    @Override
    public void setElementAt(int position, Object element) {
        load();
        block.setElementAt(position, element);
    }

    @Override
    public void resize(int positionCount) {
        load();
        block.resize(positionCount);
    }

    @Override
    public void compact(int[] selection) {
        load();
        block.compact(selection);
    }

    @Override
    public void collectNulls(int positionOffset, int positionCount, BitSet nullBitmap, int targetOffset) {
        load();
        block.collectNulls(positionOffset, positionCount, nullBitmap, targetOffset);
    }

    @Override
    public void copyToIntArray(int positionOffset, int positionCount, int[] targetArray, int targetOffset,
                               DictionaryMapping dictionaryMapping) {
        load();
        block.copyToIntArray(positionOffset, positionCount, targetArray, targetOffset, dictionaryMapping);
    }

    @Override
    public void copyToLongArray(int positionOffset, int positionCount, long[] targetArray, int targetOffset) {
        load();
        block.copyToLongArray(positionOffset, positionCount, targetArray, targetOffset);
    }

    @Override
    public void sum(int[] groupSelected, int selSize, long[] results) {
        load();
        block.sum(groupSelected, selSize, results);
    }

    @Override
    public void sum(int startIndexIncluded, int endIndexExcluded, long[] results) {
        load();
        block.sum(startIndexIncluded, endIndexExcluded, results);
    }

    @Override
    public void sum(int startIndexIncluded, int endIndexExcluded, long[] sumResultArray, int[] sumStatusArray,
                    int[] normalizedGroupIds) {
        load();
        block.sum(startIndexIncluded, endIndexExcluded, sumResultArray, sumStatusArray, normalizedGroupIds);
    }

    @Override
    public void appendTypedHashTable(TypedList typedList, int sourceIndex, int startIndexIncluded,
                                     int endIndexExcluded) {
        load();
        block.appendTypedHashTable(typedList, sourceIndex, startIndexIncluded, endIndexExcluded);
    }

    @Override
    public void count(int[] groupIds, int[] probePositions, int selSize, NullableLongGroupState state) {
        load();
        block.count(groupIds, probePositions, selSize, state);
    }

    @Override
    public void recycle() {
        load();
        block.recycle();
    }

    @Override
    public boolean isRecyclable() {
        load();
        return block.isRecyclable();
    }

    @Override
    public <T> void setRecycler(DriverObjectPool.Recycler<T> recycler) {
        load();
        block.setRecycler(recycler);
    }

    @Override
    public void addIntToBloomFilter(int totalPartitionCount, RFBloomFilter[] RFBloomFilters) {
        load();
        block.addIntToBloomFilter(totalPartitionCount, RFBloomFilters);
    }

    @Override
    public void addIntToBloomFilter(RFBloomFilter RFBloomFilter) {
        load();
        block.addIntToBloomFilter(RFBloomFilter);
    }

    @Override
    public int mightContainsInt(RFBloomFilter RFBloomFilter, boolean[] bitmap) {
        load();
        return block.mightContainsInt(RFBloomFilter, bitmap);
    }

    @Override
    public int mightContainsInt(RFBloomFilter RFBloomFilter, boolean[] bitmap, boolean isConjunctive) {
        load();
        return block.mightContainsInt(RFBloomFilter, bitmap, isConjunctive);
    }

    @Override
    public int mightContainsInt(int totalPartitionCount, RFBloomFilter[] RFBloomFilters, boolean[] bitmap,
                                boolean isPartitionConsistent) {
        load();
        return block.mightContainsInt(totalPartitionCount, RFBloomFilters, bitmap, isPartitionConsistent);
    }

    @Override
    public int mightContainsInt(int totalPartitionCount, RFBloomFilter[] RFBloomFilters, boolean[] bitmap,
                                boolean isPartitionConsistent, boolean isConjunctive) {
        load();
        return block.mightContainsInt(totalPartitionCount, RFBloomFilters, bitmap, isPartitionConsistent,
            isConjunctive);
    }

    @Override
    public void addLongToBloomFilter(int totalPartitionCount, RFBloomFilter[] RFBloomFilters) {
        load();
        block.addLongToBloomFilter(totalPartitionCount, RFBloomFilters);
    }

    @Override
    public void addLongToBloomFilter(RFBloomFilter RFBloomFilter) {
        load();
        block.addLongToBloomFilter(RFBloomFilter);
    }

    @Override
    public int mightContainsLong(RFBloomFilter RFBloomFilter, boolean[] bitmap) {
        load();
        return block.mightContainsLong(RFBloomFilter, bitmap);
    }

    @Override
    public int mightContainsLong(RFBloomFilter RFBloomFilter, boolean[] bitmap, boolean isConjunctive) {
        load();
        return block.mightContainsLong(RFBloomFilter, bitmap, isConjunctive);
    }

    @Override
    public int mightContainsLong(int totalPartitionCount, RFBloomFilter[] RFBloomFilters, boolean[] bitmap,
                                 boolean isPartitionConsistent) {
        load();
        return block.mightContainsLong(totalPartitionCount, RFBloomFilters, bitmap, isPartitionConsistent);
    }

    @Override
    public int mightContainsLong(int totalPartitionCount, RFBloomFilter[] RFBloomFilters, boolean[] bitmap,
                                 boolean isPartitionConsistent, boolean isConjunctive) {
        load();
        return block.mightContainsLong(totalPartitionCount, RFBloomFilters, bitmap, isPartitionConsistent,
            isConjunctive);
    }
}
