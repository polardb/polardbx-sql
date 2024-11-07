<@pp.dropOutputFile />

<#list cmpOperators.binaryOperators as operator>

    <#list operator.types as type>

        <#assign className = "${operator.classHeader}${type.inputDataType1}Col${type.inputDataType2}ConstVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/compare/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.compare;

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
    private final ${type.inputType2} right;
    public ${className}(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);
        Object rightValue = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        rightIsNull = (rightValue == null);
        right = rightIsNull ? (${type.inputType2})0 : (${type.inputType2}) rightValue;
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

        ${type.inputType1}[] array1 = (leftInputVectorSlot.cast(${type.inputVectorType1}.class)).${type.inputType1}Array();
		long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();

        <#if operator.classHeader != 'SEQ'>
			if (rightIsNull) {
			boolean[] outputNulls = outputVectorSlot.nulls();
			if (isSelectionInUse) {
			for (int i = 0; i < batchSize; i++) {
			int j = sel[i];
			outputNulls[j] = true;
			}
			} else {
                for (int i = 0; i < batchSize; i++) {
                    outputNulls[i] = true;
                }
            }
        } else {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    res[j] = (array1[j] ${operator.op} right) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    res[i] = (array1[i] ${operator.op} right) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }

            VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());
        }
        </#if>

        <#if operator.classHeader = 'SEQ'>
        if (rightIsNull) {
            boolean[] nulls1 = leftInputVectorSlot.nulls();
            boolean leftInputHasNull = leftInputVectorSlot.hasNull();
            boolean[] outputNulls = outputVectorSlot.nulls();
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    boolean null1 = leftInputHasNull ? nulls1[j] : false;
                    res[j] = null1 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                    outputNulls[j] = false;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    boolean null1 = leftInputHasNull ? nulls1[i] : false;
                    res[i] = null1 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                    outputNulls[i] = false;
                }
            }
        } else {
            boolean[] nulls1 = leftInputVectorSlot.nulls();
            boolean leftInputHasNull = leftInputVectorSlot.hasNull();
            boolean[] outputNulls = outputVectorSlot.nulls();
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    boolean null1 = leftInputHasNull ? nulls1[j] : false;
                    res[j] = (!null1 && (array1[j] == right)) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                    outputNulls[j] = false;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    boolean null1 = leftInputHasNull ? nulls1[i] : false;
                    res[i] = (!null1 && (array1[i] == right)) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                    outputNulls[i] = false;
                }
            }
        }
        </#if>
    }
}

    </#list>
</#list>

