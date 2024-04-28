package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.scan.IOStatus;
import com.alibaba.polardbx.executor.operator.scan.ScanState;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

public class IOStatusImpl<BATCH> implements IOStatus<BATCH> {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");
    private final String workId;

    private volatile Throwable throwable;

    /**
     * Store the IO production to concurrency-safe queue.
     */
    private ConcurrentLinkedQueue<BATCH> results;

    /**
     * Notify threads waiting for IO completion.
     */
    private List<SettableFuture<?>> blockedCallers;

    private volatile boolean isFinished;

    /**
     * NOTE: Must be changed when close the whole client.
     */
    private volatile boolean isClosed;

    /**
     * The state change action should be exclusive for state reading action.
     */
    private StampedLock lock;

    private AtomicLong rowCount = new AtomicLong(0);

    public static <BATCH> IOStatus<BATCH> create(String workId) {
        return new IOStatusImpl<>(workId);
    }

    private IOStatusImpl(String workId) {
        this.workId = workId;

        results = new ConcurrentLinkedQueue<>();
        blockedCallers = new ArrayList<>();
        isFinished = false;
        isClosed = false;
        throwable = null;
        lock = new StampedLock();
    }

    @Override
    public long rowCount() {
        return rowCount.get();
    }

    @Override
    public String workId() {
        return workId;
    }

    public ScanState state() {
        if (throwable != null) {
            return ScanState.FAILED;
        }
        if (isFinished) {
            return ScanState.FINISHED;
        }
        if (isClosed) {
            // according to the closed state of the whole client.
            return ScanState.CLOSED;
        }
        if (results.peek() != null) {
            // ready for fetching result
            return ScanState.READY;
        }
        // external consumer should be blocked to wait for IO production.
        return ScanState.BLOCKED;
    }

    public ListenableFuture<?> isBlocked() {
        throwIfFailed();
        // To be serialized with other state change actions.
        long stamp = lock.readLock();
        try {
            ScanState state = state();
            switch (state) {
            case FINISHED:
            case FAILED:
            case READY:
                notifyBlockedCallers();
                // Not blocked
                return Futures.immediateFuture(null);
            default:
                SettableFuture future = SettableFuture.create();
                blockedCallers.add(future);
                return future;
            }
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public void finish() {
        long stamp = lock.writeLock();
        try {
            isFinished = true;
            notifyBlockedCallers();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void close() {
        long stamp = lock.writeLock();
        try {
            isClosed = true;
            notifyBlockedCallers();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void addResult(BATCH result) {
        long stamp = lock.writeLock();
        try {
            results.add(result);

            // record row count
            if (result instanceof Chunk) {
                rowCount.getAndAdd(((Chunk) result).getPositionCount());
            }
            // Notify the block callers on this file read task.
            notifyBlockedCallers();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void addResults(List<BATCH> batches) {
        long stamp = lock.writeLock();
        try {
            results.addAll(batches);

            // record row count
            for (int i = 0; i < batches.size(); i++) {
                BATCH result = batches.get(i);
                if (result instanceof Chunk) {
                    rowCount.getAndAdd(((Chunk) result).getPositionCount());
                }
            }
            // Notify the block callers on this file read task.
            notifyBlockedCallers();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public BATCH popResult() {
        return results.poll();
    }

    @Override
    public void addException(Throwable t) {
        long stamp = lock.writeLock();
        try {
            if (throwable == null) {
                throwable = t;
            }
            notifyBlockedCallers();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public void throwIfFailed() {
        if (throwable != null) {
            throw GeneralUtil.nestedException(throwable);
        }
    }

    private void notifyBlockedCallers() {
        // notify all futures in list.
        for (int i = 0; i < blockedCallers.size(); i++) {
            SettableFuture<?> blockedCaller = blockedCallers.get(i);
            blockedCaller.set(null);
        }
        blockedCallers.clear();
    }

}
