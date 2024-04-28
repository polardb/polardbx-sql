package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.operator.scan.CacheReader;
import com.alibaba.polardbx.executor.operator.scan.SeekableIterator;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Cached block reader in scope of stripe.
 */
public class CacheReaderImpl implements CacheReader<Block> {
    // The stripe id.
    private final int stripeId;

    // The column id.
    private final int columnId;

    // Total count of row-groups in given stripe.
    private final int rowGroupCount;

    // inner state
    private AtomicBoolean isInitialized;

    // For initialization.
    private Map<Integer, SeekableIterator<Block>> allValidCaches;
    private Map<Integer, SeekableIterator<Block>> inFlightCaches;
    private boolean[] cachedRowGroupBitmap;

    public CacheReaderImpl(int stripeId, int columnId, int rowGroupCount) {
        this.stripeId = stripeId;
        this.columnId = columnId;
        this.rowGroupCount = rowGroupCount;
        this.isInitialized = new AtomicBoolean(false);
    }

    @Override
    public void initialize(Map<Integer, SeekableIterator<Block>> allCaches) {
        initialize(allCaches, null);
    }

    @Override
    public void initialize(Map<Integer, SeekableIterator<Block>> allValidCaches,
                           Map<Integer, SeekableIterator<Block>> inFlightCaches) {
        if (!isInitialized.compareAndSet(false, true)) {
            throw GeneralUtil.nestedException("This cache reader has already been initialized.");
        }
        this.allValidCaches = allValidCaches;
        this.inFlightCaches = inFlightCaches;

        // Record the selected row groups.
        this.cachedRowGroupBitmap = new boolean[rowGroupCount];
        Arrays.fill(cachedRowGroupBitmap, false);
        allValidCaches.keySet().stream().forEach(
            rg -> cachedRowGroupBitmap[rg] = true
        );
    }

    @Override
    public boolean isInitialized() {
        return isInitialized.get();
    }

    @Override
    public int columnId() {
        return this.columnId;
    }

    @Override
    public boolean[] cachedRowGroupBitmap() {
        Preconditions.checkArgument(isInitialized.get());
        return cachedRowGroupBitmap;
    }

    @Override
    public Map<Integer, SeekableIterator<Block>> allCaches() {
        return allValidCaches;
    }

    @Override
    public Block getCache(int groupId, int position) {
        SeekableIterator<Block> iterator = allValidCaches.get(groupId);
        if (iterator == null) {
            if (inFlightCaches == null
                || (iterator = inFlightCaches.get(groupId)) == null) {
                return null;
            }
        }
        // seek and move to next position of block.
        return iterator.seek(position);
    }
}
