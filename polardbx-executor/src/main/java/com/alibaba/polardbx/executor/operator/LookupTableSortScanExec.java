package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.mpp.split.ParamsDynamicJdbcSplit;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.join.LookupEquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTE_ON_MYSQL;

/**
 * LookupTableSortScanExec operator only support union all mode
 */
public class LookupTableSortScanExec extends TableScanSortExec implements MultiUnionAllLookupTableExec {

    private List<Split> reservedSplits;

    /**
     * for building {@code SqlNodeList}
     */
    MemoryAllocatorCtx conditionMemoryAllocator;

    private long allocatedMem = 0;

    private int inToUnionThreshold;

    public LookupTableSortScanExec(LogicalView logicalView, ExecutionContext context, TableScanClient scanClient,
                                   long maxRowCount,
                                   long skipped, long fetched,
                                   SpillerFactory spillerFactory,
                                   List<DataType> dataTypeList) {
        super(logicalView, context, scanClient, maxRowCount, skipped, fetched, spillerFactory, dataTypeList);
        this.inToUnionThreshold = context.getParamManager().getInt(ConnectionParams.IN_TO_UNION_ALL_THRESHOLD);
        Preconditions.checkArgument(
            logicalView.isInToUnionAll(), "this operator only support the in->union mode!");
    }

    @Override
    public void updateLookupPredicate(Chunk chunk) {
        if (!scanClient.noMoreSplit()) {
            throw new TddlRuntimeException(ERR_EXECUTE_ON_MYSQL, "input split not ready");
        }

        if (reservedSplits == null) {
            reservedSplits = new ArrayList<>();
            reservedSplits.addAll(scanClient.getSplitList());
        }

        scanClient.getSplitList().clear();
        if (chunk.getPositionCount() <= inToUnionThreshold) {
            updateSingleValueWhereSql(chunk, reservedSplits, scanClient);
        } else {
            updateMultiValueWhereSql(chunk, reservedSplits, scanClient);
        }
    }

    @Override
    public void setMemoryAllocator(MemoryAllocatorCtx memoryAllocator) {
        this.conditionMemoryAllocator = memoryAllocator;
    }

    @Override
    public void releaseConditionMemory() {
        conditionMemoryAllocator.releaseReservedMemory(allocatedMem, true);
        allocatedMem = 0;
    }

    @Override
    public void reserveMemory(long reservedSize) {
        if (reservedSize != 0) {
            conditionMemoryAllocator.allocateReservedMemory(reservedSize);
            allocatedMem += reservedSize;
        }
    }

    @Override
    public synchronized boolean resume() {
        this.isFinish = false;
        if (consumeResultSet != null) {
            consumeResultSet.close();
            consumeResultSet = null;
        }
        scanClient.reset();
        try {
            scanClient.executePrefetchThread(false);
        } catch (Throwable e) {
            TddlRuntimeException exception =
                new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL, e, e.getMessage());
            this.isFinish = true;
            scanClient.setException(exception);
            scanClient.throwIfFailed();
        }
        return true;
    }

    @Override
    public boolean shouldSuspend() {
        return isFinish;
    }

    @Override
    public void doSuspend() {
        if (consumeResultSet != null) {
            consumeResultSet.close();
            consumeResultSet = null;
        }
        scanClient.cancelAllThreads(false);
    }

    @Override
    synchronized void doClose() {
        super.doClose();
        releaseConditionMemory();
        this.conditionMemoryAllocator = null;
        if (reservedSplits != null) {
            reservedSplits.clear();
        }
    }
}
