package com.alibaba.polardbx.executor.operator.util;

import java.text.MessageFormat;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class DriverIntArrayPool implements DriverObjectPool<int[]> {
    private LinkedBlockingQueue<int[]> queue;
    private AtomicBoolean isCleared;
    private long recycleTimes;
    private long reuseTimes;

    public DriverIntArrayPool() {
        this.queue = new LinkedBlockingQueue<>();
        this.isCleared = new AtomicBoolean(false);
        this.recycleTimes = 0L;
        this.reuseTimes = 0L;
    }

    @Override
    public void add(int[] object) {
        queue.add(object);
        recycleTimes++;
    }

    @Override
    public int[] poll() {
        int[] res = queue.poll();
        if (res != null) {
            reuseTimes++;
        }
        return res;
    }

    @Override
    public Recycler<int[]> getRecycler(final int chunkLimit) {
        return (int[] object) -> {
            if (!isCleared.get() && object != null && object.length >= chunkLimit) {
                add(object);
            }
        };
    }

    @Override
    public void clear() {
        if (isCleared.compareAndSet(false, true)) {
            queue.clear();
        }
    }

    @Override
    public String report() {
        return MessageFormat.format("int array object pool, recycleTimes = {0}, reuseTimes = {1}", recycleTimes,
            reuseTimes);
    }
}
