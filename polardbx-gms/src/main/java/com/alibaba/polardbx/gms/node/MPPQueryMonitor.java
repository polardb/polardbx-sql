package com.alibaba.polardbx.gms.node;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.sync.SyncScope;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

/**
 * Calculate the QPS of queries under MPP mode within the statistical time window.
 * Additionally, calculate the concurrency of queries under MPP mode.
 */
public class MPPQueryMonitor {
    private static int nodeSize;

    static {
        // Get node size of master CN cluster.
        if (ConfigDataMode.isMasterMode()) {
            List<GmsNodeManager.GmsNode> nodes =
                GmsNodeManager.getInstance().getNodesBySyncScope(SyncScope.MASTER_ONLY);
            nodeSize = nodes.size();
        } else {
            nodeSize = 0;
        }
    }

    private static final MPPQueryMonitor INSTANCE = new MPPQueryMonitor();

    private AtomicLong currentQueries = new AtomicLong(0L);
    private ConcurrentLinkedQueue<Long> queryFinishedTimestamp = new ConcurrentLinkedQueue<>();
    private StampedLock stampedLock = new StampedLock();

    public static MPPQueryMonitor getInstance() {
        return INSTANCE;
    }

    public void recordStartingQuery() {
        currentQueries.getAndIncrement();
    }

    public void recordFinishedQuery(long windowPeriodInMillis) {
        long now = System.currentTimeMillis();
        queryFinishedTimestamp.add(now);
        currentQueries.decrementAndGet();

        // Remove timestamps that exceed the statistical period.
        long stamp = stampedLock.writeLock();
        try {
            while (!queryFinishedTimestamp.isEmpty()
                && now - queryFinishedTimestamp.peek() > windowPeriodInMillis) {
                queryFinishedTimestamp.poll();
            }
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    public double calculateQPS(long windowPeriodInMillis) {
        // Calculate the QPS within the statistical period.
        double qps = 0;
        if (!queryFinishedTimestamp.isEmpty()
            && System.currentTimeMillis() - queryFinishedTimestamp.peek() <= windowPeriodInMillis) {
            qps = queryFinishedTimestamp.size() / (windowPeriodInMillis / 1000.0);
        }
        return qps * nodeSize;
    }

    public long getQueryConcurrency() {
        return currentQueries.get() * nodeSize;
    }
}