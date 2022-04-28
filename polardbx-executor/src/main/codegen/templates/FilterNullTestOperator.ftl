<@pp.dropOutputFile />

<#list cmpOperators.nullTestOperators.operators as operator>

    <#list cmpOperators.nullTestOperators.types as type>

        <#assign className = "Filter${operator.classHeader}${type.inputDataType}ColVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/comparison/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.comparison;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.alibaba.polardbx.executor.vectorized.*;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionMode;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.*;
import com.alibaba.polardbx.executor.chunk.*;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType}"}, argumentKinds = {Variable}, mode = ExpressionMode.FILTER)
public class ${className} extends AbstractVectorizedExpression {
    public ${className}(int outputIndex, VectorizedExpression[] children) {
        super(null, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        super.evalChildren(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock inputVectorSlot = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        ${type.inputType}[] inputArray = ((${type.inputVectorType}) inputVectorSlot).${type.inputType}Array();
        boolean[] inputNulls = inputVectorSlot.nulls();

        int newSize = 0;
        if (inputVectorSlot.hasNull()) {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    <#if operator.classHeader = "IsNull">
                    if (inputNulls[j]) {
                    </#if>
                    <#if operator.classHeader = "IsNotNull">
                    if (!inputNulls[j]) {
                    </#if>
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    <#if operator.classHeader = "IsNull">
                    if (inputNulls[i]) {
                    </#if>
                    <#if operator.classHeader = "IsNotNull">
                    if (!inputNulls[i]) {
                    </#if>
                        sel[newSize++] = i;
                    }
                }
            }
        } else {
            <#if operator.classHeader = "IsNull">
            // do nothing
            </#if>
            <#if operator.classHeader = "IsNotNull">
            newSize = batchSize;
            </#if>
        }
        if(newSize < batchSize) {
            chunk.setBatchSize(newSize);
            chunk.setSelectionInUse(true);
        }
    }
}
    </#list>
</#list>

