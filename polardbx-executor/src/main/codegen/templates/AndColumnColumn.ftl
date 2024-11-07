<@pp.dropOutputFile />

<#list conjunctionOperators.operators as operator>

        <#assign className = "AndLongCol${operator.operandCount}VectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/compare/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionPriority;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.Arrays;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
@ExpressionSignatures(
    names = {"AND"},
    argumentTypes = {
        <#list 1..(operator.operandCount) as i>
            <#if i == (operator.operandCount)>
                "Long"
            <#else>
                "Long",
            </#if>
        </#list>
        },
    argumentKinds = {
        <#list 1..(operator.operandCount) as i>
            <#if i == (operator.operandCount)>
                Variable
            <#else>
                Variable,
            </#if>
        </#list>
})
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
        <#list 1..(operator.operandCount) as i>
        RandomAccessBlock inputVec${i} = chunk.slotIn(children[${i-1}].getOutputIndex(), children[${i-1}].getOutputDataType());
        </#list>

        <#list 1..(operator.operandCount) as i>
            long[] array${i} = (inputVec${i}.cast(LongBlock.class)).longArray();
            boolean[] nulls${i} = inputVec${i}.nulls();
            boolean input${i}HasNull = inputVec${i}.hasNull();

        </#list>

        long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();
        boolean[] outputNulls = outputVectorSlot.nulls();

        boolean outputVectorHasNull = false;
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                <#list 1..(operator.operandCount) as c>
                    boolean null${c} = !input${c}HasNull ? false : nulls${c}[j];
                </#list>

                <#list 1..(operator.operandCount) as c>
                    boolean b${c} = (array${c}[j] != 0);
                </#list>

                boolean hasNull = null1;
                <#list 2..(operator.operandCount) as c>
                    hasNull |= null${c};
                </#list>

                boolean anyFalse = (!null1 && !b1);
                <#list 2..(operator.operandCount) as c>
                    anyFalse |= (!null${c} && !b${c});
                </#list>

                outputNulls[j] = !anyFalse && hasNull;
                res[j] = !anyFalse && !hasNull ? 1 : 0;

                outputVectorHasNull |= outputNulls[j];
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                <#list 1..(operator.operandCount) as c>
                boolean null${c} = !input${c}HasNull ? false : nulls${c}[i];
                </#list>

                <#list 1..(operator.operandCount) as c>
                boolean b${c} = (array${c}[i] != 0);
                </#list>

                boolean hasNull = null1;
                <#list 2..(operator.operandCount) as c>
                hasNull |= null${c};
                </#list>

                boolean anyFalse = (!null1 && !b1);
                <#list 2..(operator.operandCount) as c>
                anyFalse |= (!null${c} && !b${c});
                </#list>

                outputNulls[i] = !anyFalse && hasNull;
                res[i] = !anyFalse && !hasNull ? 1 : 0;

                outputVectorHasNull |= outputNulls[i];
            }
        }

        outputVectorSlot.setHasNull(outputVectorHasNull);

    }
}

</#list>

