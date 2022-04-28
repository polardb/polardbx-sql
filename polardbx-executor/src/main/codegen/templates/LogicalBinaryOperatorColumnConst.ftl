<@pp.dropOutputFile />

<#list logicalOperators.binaryOperators.operators as operator>

    <#list logicalOperators.binaryOperators.types as type>

        <#assign className = "${operator.classHeader}${type.inputDataType1}Col${type.inputDataType2}ConstVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/logical/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.logical;

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
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType1}", "${type.inputDataType2}"}, argumentKinds = {Variable, Const})
public class ${className} extends AbstractVectorizedExpression {
    private final boolean rightIsNull;
    private final boolean right;
    public ${className}(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);
        Object rightValue = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        rightIsNull = (rightValue == null);
        right = rightIsNull ? false : (((${type.inputType2}) rightValue) != 0);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock leftInputVectorSlot = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        ${type.inputType1}[] array1 = ((${type.inputVectorType1}) leftInputVectorSlot).${type.inputType1}Array();
        boolean[] nulls1 = leftInputVectorSlot.nulls();
        boolean leftInputHasNull = leftInputVectorSlot.hasNull();
        long[] res = ((LongBlock) outputVectorSlot).longArray();
        boolean[] outputNulls = outputVectorSlot.nulls();
        outputVectorSlot.setHasNull(leftInputVectorSlot.hasNull() | rightIsNull);

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                boolean null1 = !leftInputHasNull ? false : nulls1[j];
                boolean b1 = (array1[j] != 0);

                <#if operator.classHeader = "And">
                outputNulls[j] = (null1 && rightIsNull) || (null1 && right) || (rightIsNull && b1);
                res[j] = ((!null1 && !b1) || (!rightIsNull && !right)) ? LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
                </#if>
                <#if operator.classHeader = "Or">
                outputNulls[j] = (null1 && rightIsNull) || (null1 && !right) || (rightIsNull && !b1);
                res[j] = ((!null1 && b1) || (!rightIsNull && right)) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
                <#if operator.classHeader = "Xor">
                outputNulls[j] = null1 || rightIsNull;
                res[j] = b1^right ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                boolean null1 = !leftInputHasNull ? false : nulls1[i];
                boolean b1 = (array1[i] != 0);

                <#if operator.classHeader = "And">
                outputNulls[i] = (null1 && rightIsNull) || (null1 && right) || (rightIsNull && b1);
                res[i] = ((!null1 && !b1) || (!rightIsNull && !right)) ? LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
                </#if>
                <#if operator.classHeader = "Or">
                outputNulls[i] = (null1 && rightIsNull) || (null1 && !right) || (rightIsNull && !b1);
                res[i] = ((!null1 && b1) || (!rightIsNull && right)) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
                <#if operator.classHeader = "Xor">
                outputNulls[i] = null1 || rightIsNull;
                res[i] = b1^right ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
            }
        }
    }
}
    </#list>
</#list>

