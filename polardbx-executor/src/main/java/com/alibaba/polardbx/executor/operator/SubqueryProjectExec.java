//package com.alibaba.polardbx.executor.operator;
//
//import com.google.common.base.Preconditions;
//import com.google.common.util.concurrent.ListenableFuture;
//import com.alibaba.polardbx.optimizer.chunk.Chunk;
//import com.alibaba.polardbx.executor.utils.SubqueryApply;
//import com.alibaba.polardbx.executor.utils.SubqueryUtils;
//import ColumnMeta;
//import ExecutionContext;
//import DynamicParamExpression;
//import IExpression;
//import org.apache.calcite.rel.core.CorrelationId;
//
//import java.util.List;
//import java.util.Queue;
//import java.util.concurrent.ConcurrentLinkedQueue;
//
//public class SubqueryProjectExec extends ProjectExec implements ProducerExecutor {
//
//    private ProducerStatus inputStatus;
//    private Chunk currentChunk;
//
//    private boolean isFinish;
//    private ListenableFuture<?> blocked;
//
//    private final Queue<SubqueryApply> subqueryApply = new ConcurrentLinkedQueue<>();
//    private SubqueryApply curSubqueryApply = null;
//    private int applyRowIndex = 0;
//    private boolean closed;
//
//    // Fields for iterative execution for subquery
//    protected CorrelationId correlateId;
//    protected List<DynamicParamExpression> applyValue;
//    protected boolean isApplyValue;
//
//    public SubqueryProjectExec(Executor input, List<IExpression> expressions, List<ColumnMeta> columns,
//                               CorrelationId correlateId, List<DynamicParamExpression> dynamicExpressions,
//                               ExecutionContext context) {
//        super(input, expressions, columns, context);
//
//        this.correlateId = correlateId;
//        this.applyValue = dynamicExpressions;
//        this.isApplyValue = applyValue != null && applyValue.size() > 0;
//        Preconditions.checkArgument(isApplyValue, "SubqueryProjectExec only support for subquery");
//        this.inputStatus = new ProducerStatus(input);
//        this.blocked = NOT_BLOCKED;
//    }
//
//    private void createSubqueryApply(int rowIndex) {
//        if (rowIndex < currentChunk.getPositionCount()) {
//            Chunk.ChunkRow chunkRow = currentChunk.rowAt(rowIndex);
//            // handle apply subquerys in projects
//            for (DynamicParamExpression expr : applyValue) {
//                subqueryApply
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
//        if (currentChunk == null) {
//            // read next chunk from input
//            currentChunk = input.nextChunk();
//            if (currentChunk == null) {
//                isFinish = inputStatus.isFinished();
//                blocked = inputStatus.produceIsBlocked();
//                return null;
//            }
//            applyRowIndex = 0;
//        }
//        if (curSubqueryApply == null) {
//            createSubqueryApply(applyRowIndex);
//            if (subqueryApply.isEmpty()) {
//                // apply of inputRow is finished, prcess next chunk.
//                currentChunk = null;
//                return null;
//            }
//            curSubqueryApply = subqueryApply.poll();
//            curSubqueryApply.prepare();
//        }
//        curSubqueryApply.process();
//        if (curSubqueryApply.isFinished()) {
//            curSubqueryApply.close();
//            if (subqueryApply.isEmpty()) {
//                // apply of applyRowIndex's row is finished, compute result
//                Chunk.ChunkRow chunkRow = currentChunk.rowAt(applyRowIndex);
//                for (int c = 0; c < columns.size(); c++) {
//                    if (mappedColumnIndex[c] < 0) {
//                        // Need to evaluate the expression
//                        super.evaluateExpression(chunkRow, c);
//                    }
//                }
//                applyRowIndex++;
//                curSubqueryApply = null;
//                if (applyRowIndex == currentChunk.getPositionCount()) {
//                    // Apply of currentChunk is finished
//                    Chunk ret = super.buildChunk(currentChunk);
//                    currentChunk = null;
//                    return ret;
//                }
//            } else {
//                curSubqueryApply = subqueryApply.poll();
//                curSubqueryApply.prepare();
//            }
//        } else {
//            blocked = curSubqueryApply.isBlocked();
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
//        currentChunk = null;
//        if (curSubqueryApply != null) {
//            curSubqueryApply.close();
//            curSubqueryApply = null;
//        }
//        SubqueryApply apply;
//        while ((apply = subqueryApply.poll()) != null) {
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
