<@pp.dropOutputFile />

<#list logicalOperators.binaryOperators.operators as operator>

    <#list logicalOperators.binaryOperators.types as type>

        <#assign className = "${operator.classHeader}${type.inputDataType1}Const${type.inputDataType2}ColVectorizedExpression">
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
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType1}", "${type.inputDataType2}"}, argumentKinds = {Const, Variable})
public class ${className} extends AbstractVectorizedExpression {
    private final boolean leftIsNull;
    private final boolean left;
    public ${className}(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);
        Object leftValue = ((LiteralVectorizedExpression) children[0]).getConvertedValue();
        leftIsNull = (leftValue == null);
        left = leftIsNull ? false : (((${type.inputType1}) leftValue) != 0);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[1].eval(ctx);

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock leftInputVectorSlot = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        RandomAccessBlock rightInputVectorSlot = chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        ${type.inputType2}[] array2 = ((${type.inputVectorType2}) rightInputVectorSlot).${type.inputType2}Array();
        boolean[] nulls2 = rightInputVectorSlot.nulls();
        boolean rightInputHasNull = rightInputVectorSlot.hasNull();
        long[] res = ((LongBlock) outputVectorSlot).longArray();
        boolean[] outputNulls = outputVectorSlot.nulls();
        outputVectorSlot.setHasNull(leftIsNull | rightInputVectorSlot.hasNull());

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                boolean null2 = !rightInputHasNull ? false : nulls2[j];
                boolean b2 = (array2[j] != 0);

                <#if operator.classHeader = "And">
                outputNulls[j] = (leftIsNull && null2) || (leftIsNull && b2) || (null2 && left);
                res[j] = ((!leftIsNull && !left) || (!null2 && !b2)) ? LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
                </#if>
                <#if operator.classHeader = "Or">
                outputNulls[j] = (leftIsNull && null2) || (leftIsNull && !b2) || (null2 && !left);
                res[j] = ((!leftIsNull && left) || (!null2 && b2)) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
                <#if operator.classHeader = "Xor">
                outputNulls[j] = leftIsNull || null2;
                res[j] = left^b2 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                boolean null2 = !rightInputHasNull ? false : nulls2[i];
                boolean b2 = (array2[i] != 0);

                <#if operator.classHeader = "And">
                outputNulls[i] = (leftIsNull && null2) || (leftIsNull && b2) || (null2 && left);
                res[i] = ((!leftIsNull && !left) || (!null2 && !b2)) ? LongBlock.FALSE_VALUE : LongBlock.TRUE_VALUE;
                </#if>
                <#if operator.classHeader = "Or">
                outputNulls[i] = (leftIsNull && null2) || (leftIsNull && !b2) || (null2 && !left);
                res[i] = ((!leftIsNull && left) || (!null2 && b2)) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
                <#if operator.classHeader = "Xor">
                 outputNulls[i] = leftIsNull || null2;
                 res[i] = left^b2 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
            }
        }
    }
}

    </#list>
</#list>

