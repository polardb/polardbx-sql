<@pp.dropOutputFile />
<#assign argTypes = ["Datetime", "Timestamp"]>
<#list argTypes as argType1>
    <#list argTypes as argType2>
        <#list cmpDateOperators.operators as operator>
        <#assign className = "${operator.classHeader}${argType1}Col${argType2}ColVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/compare/${className}.java" />

package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.TimestampBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

/*
 * This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${argType1}", "${argType2}"},
    argumentKinds = {ArgumentKind.Variable, ArgumentKind.Variable})
public class ${className} extends AbstractVectorizedExpression {

    public ${className}(
        int outputIndex,
        VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        super.evalChildren(ctx);

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        RandomAccessBlock rightInputVectorSlot =
            chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        if (leftInputVectorSlot instanceof TimestampBlock && rightInputVectorSlot instanceof TimestampBlock) {
            long[] array1 = ((TimestampBlock) leftInputVectorSlot).getPacked();
            long[] array2 = ((TimestampBlock) rightInputVectorSlot).getPacked();
            long[] res = ((LongBlock) outputVectorSlot).longArray();

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    res[j] = (array1[j] ${operator.op} array2[j]) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    res[i] = (array1[i] ${operator.op} array2[i]) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }
            VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex(),
                children[1].getOutputIndex());
        } else if (leftInputVectorSlot instanceof ReferenceBlock && rightInputVectorSlot instanceof ReferenceBlock) {
            long[] res = ((LongBlock) outputVectorSlot).longArray();

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    long lPack = VectorizedExpressionUtils.packedLong(leftInputVectorSlot, j);
                    long rPack = VectorizedExpressionUtils.packedLong(rightInputVectorSlot, j);

                    res[j] = lPack ${operator.op} rPack ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    long lPack = VectorizedExpressionUtils.packedLong(leftInputVectorSlot, i);
                    long rPack = VectorizedExpressionUtils.packedLong(rightInputVectorSlot, i);

                    res[i] = lPack ${operator.op} rPack ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }

            VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex(),
                children[1].getOutputIndex());
        } else if (leftInputVectorSlot instanceof TimestampBlock && rightInputVectorSlot instanceof ReferenceBlock) {
            long[] res = ((LongBlock) outputVectorSlot).longArray();
            long[] array1 = ((TimestampBlock) leftInputVectorSlot).getPacked();

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    long rPack = VectorizedExpressionUtils.packedLong(rightInputVectorSlot, j);
                    res[j] = (array1[j] ${operator.op} rPack) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    long rPack = VectorizedExpressionUtils.packedLong(rightInputVectorSlot, i);
                    res[i] = (array1[i] ${operator.op} rPack) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }

            VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex(),
                children[1].getOutputIndex());
        } else if (leftInputVectorSlot instanceof ReferenceBlock && rightInputVectorSlot instanceof TimestampBlock) {
            long[] res = ((LongBlock) outputVectorSlot).longArray();
            long[] array2 = ((TimestampBlock) rightInputVectorSlot).getPacked();

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    long lPack = VectorizedExpressionUtils.packedLong(leftInputVectorSlot, j);
                    res[j] = (lPack ${operator.op} array2[j]) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    long lPack = VectorizedExpressionUtils.packedLong(leftInputVectorSlot, i);
                    res[i] = (lPack ${operator.op} array2[i]) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }

            VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex(),
                children[1].getOutputIndex());
        }
    }
}

        </#list>
    </#list>
</#list>
