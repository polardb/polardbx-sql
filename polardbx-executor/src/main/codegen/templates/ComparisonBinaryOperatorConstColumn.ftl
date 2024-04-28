<@pp.dropOutputFile />

<#list cmpOperators.binaryOperators as operator>

    <#list operator.types as type>

        <#assign className = "${operator.classHeader}${type.inputDataType1}Const${type.inputDataType2}ColVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/comparison/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.alibaba.polardbx.executor.vectorized.*;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.*;
import com.alibaba.polardbx.executor.chunk.*;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType1}", "${type.inputDataType2}"}, argumentKinds = {Const, Variable})
public class ${className} extends AbstractVectorizedExpression {
    private final boolean leftIsNull;
    private final ${type.inputType1} left;
    public ${className}(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);
        Object leftValue = ((LiteralVectorizedExpression) children[0]).getConvertedValue();
        leftIsNull = leftValue == null;
        left = leftIsNull ? (${type.inputType1}) 0: (${type.inputType1})leftValue ;
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[1].eval(ctx);

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock rightInputVectorSlot = chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        ${type.inputType2}[] array2 = (rightInputVectorSlot.cast(${type.inputVectorType2}.class)).${type.inputType2}Array();
        long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();

        <#if operator.classHeader != "SEQ">
            if (leftIsNull) {
            boolean[] outputNulls = outputVectorSlot.nulls();
            if (isSelectionInUse) {
            for (int i=0; i < batchSize; i++) {
            int j = sel[i];
            outputNulls[j] = true;
            }
            } else {
                for (int i=0; i < batchSize; i++) {
                    outputNulls[i] = true;
                }
            }
        } else {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    res[j] = (left ${operator.op} array2[j]) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    res[i] = (left ${operator.op} array2[i]) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }
            VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[1].getOutputIndex());
        }
        </#if>

        <#if operator.classHeader = "SEQ">
            if (leftIsNull) {
                boolean[] nulls2 = rightInputVectorSlot.nulls();
                boolean rightInputHasNull = rightInputVectorSlot.hasNull();
                boolean[] outputNulls = outputVectorSlot.nulls();
                if (isSelectionInUse) {
                    for (int i=0; i < batchSize; i++) {
                        int j = sel[i];
                        boolean null2 = rightInputHasNull ? nulls2[j] : false;
                        res[j] = null2 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                        outputNulls[j] = false;
                    }
                } else {
                    for (int i=0; i < batchSize; i++) {
                        boolean null2 = rightInputHasNull ? nulls2[i] : false;
                        res[i] = null2 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                        outputNulls[i] = false;
                    }
                }
            } else {
                boolean[] nulls2 = rightInputVectorSlot.nulls();
                boolean rightInputHasNull = rightInputVectorSlot.hasNull();
                boolean[] outputNulls = outputVectorSlot.nulls();
                if (isSelectionInUse) {
                    for (int i = 0; i < batchSize; i++) {
                        int j = sel[i];
                        boolean null2 = rightInputHasNull ? nulls2[j] : false;
                        res[j] = (!null2 && (left == array2[j])) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                        outputNulls[j] = false;
                    }
                } else {
                    for (int i = 0; i < batchSize; i++) {
                        boolean null2 = rightInputHasNull ? nulls2[i] : false;
                        res[i] = (!null2 && (left == array2[i])) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                        outputNulls[i] = false;
                    }
                }
            }
        </#if>
    }
}

    </#list>
</#list>

