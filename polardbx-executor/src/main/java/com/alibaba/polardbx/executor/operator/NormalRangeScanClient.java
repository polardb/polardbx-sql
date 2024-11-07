package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.executor.mpp.operator.RangeScanMode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yuehan.wcf
 */
public class NormalRangeScanClient extends TableScanClient {

    Map<Integer, SplitResultSet> notReadyResultSet = new HashMap<>();

    AtomicInteger sequence = new AtomicInteger(0);

    public NormalRangeScanClient(ExecutionContext context, CursorMeta meta,
                                 boolean useTransaction, int prefetchNum, RangeScanMode rangeScanMode) {
        super(context, meta, useTransaction, prefetchNum, rangeScanMode);
    }

    /**
     * Adds a split result set to the current object.
     * This method checks whether the incoming result set is ready to be added to the ready result sets and, if so, adds it and notifies waiting callers.
     * The method is synchronized to handle concurrent access in a multi-threaded environment.
     *
     * @param splitResultSet The split result set to be added, containing the split index and result data.
     */
    @Override
    public synchronized void addSplitResultSet(TableScanClient.SplitResultSet splitResultSet) {
        boolean resultReady = false;
        // Check if the incoming result set is the next expected one; if so, add it to the ready result sets
        if (splitResultSet.splitIndex == sequence.get()) {
            readyResultSet.add(splitResultSet);
            sequence.incrementAndGet();
            resultReady = true;
        } else {
            // If not the expected result set, place it in the queue of unready result sets awaiting their turn
            notReadyResultSet.put(splitResultSet.splitIndex, splitResultSet);
        }
        // Move all result sets that have reached their sequence from the unready queue to the ready queue
        while (notReadyResultSet.containsKey(sequence.get())) {
            SplitResultSet result = notReadyResultSet.get(sequence.get());
            readyResultSet.add(result);
            notReadyResultSet.remove(sequence.get());
            sequence.incrementAndGet();
            resultReady = true;
        }
        // If any result sets became ready, notify waiting callers
        if (resultReady) {
            notifyBlockedCallers();
        }
    }

    /**
     * Clears the result sets.
     * This method resets both the readyResultSet and notReadyResultSet, and initializes the sequence number.
     * It is synchronized to ensure safe operations in a multi-threaded environment.
     */
    @Override
    protected synchronized void clearResultSet() {
        readyResultSet.clear();
        notReadyResultSet.clear();
        sequence = new AtomicInteger(0);
    }
}
