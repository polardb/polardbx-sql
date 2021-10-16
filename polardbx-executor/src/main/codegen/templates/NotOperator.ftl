<@pp.dropOutputFile />

<#list logicalOperators.notTypes as type>

        <#assign className = "Not${type.inputDataType}ColVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/logical/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.logical;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.alibaba.polardbx.executor.vectorized.*;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.*;
import com.alibaba.polardbx.optimizer.chunk.*;
import com.alibaba.polardbx.optimizer.context.EvaluationContext;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
@ExpressionSignatures(names = {"!", "not"}, argumentTypes = {"${type.inputDataType}"}, argumentKinds = {Variable})
public class ${className} extends AbstractVectorizedExpression {
    public ${className}(int outputIndex, VectorizedExpression[] children) {
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
        RandomAccessBlock inputVectorSlot = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        ${type.inputType}[] inputArray = ((${type.inputVectorType}) inputVectorSlot).${type.inputType}Array();
        long[] res = ((LongBlock) outputVectorSlot).longArray();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                res[j] = (inputArray[j] == 0) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                res[i] = (inputArray[i] == 0) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());
    }
}
</#list>

