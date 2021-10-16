<@pp.dropOutputFile />

<#list logicalOperators.binaryOperators.operators as operator>

    <#list logicalOperators.binaryOperators.types as type>

        <#assign className = "${operator.classHeader}${type.inputDataType1}Col${type.inputDataType2}ColVectorizedExpression">
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
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType1}", "${type.inputDataType2}"}, argumentKinds = {Variable, Variable})
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
        RandomAccessBlock leftInputVectorSlot = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        RandomAccessBlock rightInputVectorSlot = chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        ${type.inputType1}[] array1 = ((${type.inputVectorType1}) leftInputVectorSlot).${type.inputType1}Array();
        boolean[] nulls1 = leftInputVectorSlot.nulls();
        boolean leftInputHasNull = leftInputVectorSlot.hasNull();
        ${type.inputType2}[] array2 = ((${type.inputVectorType2}) rightInputVectorSlot).${type.inputType2}Array();
        boolean[] nulls2 = rightInputVectorSlot.nulls();
        boolean rightInputHasNull = rightInputVectorSlot.hasNull();
        long[] res = ((LongBlock) outputVectorSlot).longArray();
        boolean[] outputNulls = outputVectorSlot.nulls();
        outputVectorSlot.setHasNull(leftInputVectorSlot.hasNull() | rightInputVectorSlot.hasNull());

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                boolean null1 = !leftInputHasNull ? false : nulls1[j];
                boolean null2 = !rightInputHasNull ? false : nulls2[j];
                boolean b1 = (array1[j] != 0);
                boolean b2 = (array2[j] != 0);

                <#if operator.classHeader = "And">
                outputNulls[j] = (null1 && null2) || (null1 && b2) || (null2 && b1);
                res[j] = ((!null1 && !b1) || (!null2 && !b2)) ? LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
                </#if>
                <#if operator.classHeader = "Or">
                outputNulls[j] = (null1 && null2) || (null1 && !b2) || (null2 && !b1);
                res[j] = ((!null1 && b1) || (!null2 && b2)) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
                <#if operator.classHeader = "Xor">
                outputNulls[j] = null1 || null2;
                res[j] = b1^b2 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                boolean null1 = !leftInputHasNull ? false : nulls1[i];
                boolean null2 = !rightInputHasNull ? false : nulls2[i];
                boolean b1 = (array1[i] != 0);
                boolean b2 = (array2[i] != 0);

                <#if operator.classHeader = "And">
                outputNulls[i] = (null1 && null2) || (null1 && b2) || (null2 && b1);
                res[i] = ((!null1 && !b1) || (!null2 && !b2)) ? LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
                </#if>
                <#if operator.classHeader = "Or">
                outputNulls[i] = (null1 && null2) || (null1 && !b2) || (null2 && !b1);
                res[i] = ((!null1 && b1) || (!null2 && b2)) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
                <#if operator.classHeader = "Xor">
                 outputNulls[i] = null1 || null2;
                 res[i] = b1^b2 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
            }
        }
    }
}

    </#list>
</#list>

