//package com.alibaba.polardbx.executor.operator;
//
//import com.google.common.base.Preconditions;
//import com.google.common.util.concurrent.ListenableFuture;
//import GeneralUtil;
//import com.alibaba.polardbx.executor.chunk.Chunk;
//import com.alibaba.polardbx.executor.operator.util.BloomFilterExpression;
//import com.alibaba.polardbx.executor.utils.ConditionUtils;
//import com.alibaba.polardbx.executor.utils.SubqueryApply;
//import com.alibaba.polardbx.executor.utils.SubqueryUtils;
//import ExecutionContext;
//import DynamicParamExpression;
//import IExpression;
//import org.apache.calcite.rel.core.CorrelationId;
//
//import java.util.List;
//import java.util.Queue;
//import java.util.concurrent.ConcurrentLinkedQueue;
//
//public class SubqueryFilterExec extends FilterExec implements ProducerExecutor {
//
//    private ProducerStatus inputStatus;
//    private boolean isFinish;
//    private ListenableFuture<?> blocked;
//
//    private final Queue<SubqueryApply> curRowApplyList = new ConcurrentLinkedQueue<>();
//    private SubqueryApply currentApply = null;
//    private int applyRowIndex = 0;
//    private int rowBufferCnt = 0;
//    private boolean closed;
//    // Fields for iterative execution of subquery
//    protected CorrelationId correlateId;
//    protected List<DynamicParamExpression> applyValue;
//    protected boolean isApplyValue;
//
//    public SubqueryFilterExec(Executor input, IExpression condition, CorrelationId correlateId,
//                              List<DynamicParamExpression> dynamicExprs,
//                              BloomFilterExpression bloomFilterExpression,
//                              ExecutionContext context) {
//        super(input, condition, bloomFilterExpression, context);
//        this.correlateId = correlateId;
//        this.applyValue = dynamicExprs;
//        this.isApplyValue = applyValue != null && applyValue.size() > 0;
//        Preconditions.checkArgument(isApplyValue, "SubqueryFilterExec only support for subquery");
//        this.inputStatus = new ProducerStatus(input);
//        this.blocked = NOT_BLOCKED;
//    }
//
//    private void createSubqueryApply(int rowIndex) {
//        if (rowIndex < inputChunk.getPositionCount()) {
//            Chunk.ChunkRow chunkRow = inputChunk.rowAt(rowIndex);
//            // handle apply subquerys in projects
//            for (DynamicParamExpression expr : applyValue) {
//                curRowApplyList
//                    .add(SubqueryUtils.createCorrelateSubqueryApply(chunkRow, expr, context, correlateId, true));
//            }
//        }
//    }
//
//    @Override
//    Chunk doNextChunk() {
//        if (closed) {
//            forceClose();
//            return null;
//        }
//        if (inputChunk == null) {
//            // read next chunk from input
//            inputChunk = input.nextChunk();
//            if (inputChunk == null) {
//                isFinish = inputStatus.isFinished();
//                blocked = inputStatus.produceIsBlocked();
//                if (isFinish && rowBufferCnt > 0) {
//                    // if input has no data, it need return buffer data.
//                    rowBufferCnt = 0;
//                    return buildChunkAndReset();
//                }
//                return null;
//            }
//            applyRowIndex = 0;
//        }
//        if (currentApply == null) {
//            // create new apply for nextRow in inputRow
//            createSubqueryApply(applyRowIndex);
//            if (curRowApplyList.isEmpty()) {
//                // apply of inputRow is finished, prcess next chunk.
//                inputChunk = null;
//                return null;
//            }
//            currentApply = curRowApplyList.poll();
//            currentApply.prepare();
//        }
//        currentApply.process();
//        if (currentApply.isFinished()) {
//            currentApply.close();
//            if (curRowApplyList.isEmpty()) {
//                // apply of applyRowIndex's row is finished, compute condition
//                Chunk.ChunkRow chunkRow = inputChunk.rowAt(applyRowIndex);
//                Object result = condition.eval(chunkRow);
//                boolean resultBoolean = ConditionUtils.convertConditionToBoolean(result);
//                if (resultBoolean) {
//                    for (int c = 0; c < blockBuilders.length; c++) {
//                        inputChunk.getBlock(c).writePositionTo(applyRowIndex, blockBuilders[c]);
//                    }
//                    rowBufferCnt++;
//                }
//                applyRowIndex++;
//                currentApply = null;
//                if (applyRowIndex == inputChunk.getPositionCount()) {
//                    // Apply of currentChunk is finished, need for next chunk
//                    inputChunk = null;
//                }
//                if (rowBufferCnt >= chunkLimit) {
//                    rowBufferCnt = 0;
//                    return buildChunkAndReset();
//                }
//            } else {
//                currentApply = curRowApplyList.poll();
//                currentApply.prepare();
//            }
//        } else {
//            blocked = currentApply.isBlocked();
//        }
//        return null;
//    }
//
//    @Override
//    void doClose() {
//        super.doClose();
//        closed = true;
//        isFinish = true;
//        forceClose();
//    }
//
//    @Override
//    public synchronized void forceClose() {
//        inputChunk = null;
//        if (currentApply != null) {
//            currentApply.close();
//            currentApply = null;
//        }
//        SubqueryApply apply;
//        while ((apply = curRowApplyList.poll()) != null) {
//            apply.close();
//        }
//    }
//
//    @Override
//    public boolean produceIsFinished() {
//        return isFinish;
//    }
//
//    @Override
//    public ListenableFuture<?> produceIsBlocked() {
//        return blocked;
//    }
//}