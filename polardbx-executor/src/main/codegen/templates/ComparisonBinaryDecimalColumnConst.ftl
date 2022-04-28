<@pp.dropOutputFile />

<#list cmpDecOperators.operators as operator>

    <#list operator.types as type>

        <#assign className = "${operator.classHeader}DecimalCol${type.constType}ConstVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/compare/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

/*
 * This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"Decimal", "${type.constType}"}, argumentKinds = {Variable, Const})
public class ${className} extends AbstractVectorizedExpression {
    private final boolean operand1IsNull;
    private final Decimal operand1;
    public ${className}(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);

        Object operand1Value = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        if (operand1Value == null) {
            operand1IsNull = true;
            operand1 = Decimal.ZERO;
        } else {
            operand1IsNull = false;
            operand1 = DataTypes.DecimalType.convertFrom(operand1Value);
        }
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        long[] output = ((LongBlock) outputVectorSlot).longArray();

        DecimalStructure leftDec;
        DecimalStructure operand1Dec = operand1.getDecimalStructure();

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                int fromIndex = j * DECIMAL_MEMORY_SIZE;

                // fetch left decimal value
                leftDec = new DecimalStructure(((DecimalBlock) leftInputVectorSlot).getRegion(j));

                boolean b1 = FastDecimalUtils.compare(leftDec, operand1Dec) ${operator.op} 0;

                output[j] = b1 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                int fromIndex = i * DECIMAL_MEMORY_SIZE;

                // fetch left decimal value
                leftDec = new DecimalStructure(((DecimalBlock) leftInputVectorSlot).getRegion(i));

                boolean b1 = FastDecimalUtils.compare(leftDec, operand1Dec) ${operator.op} 0;

                output[i] = b1 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
    }
}

    </#list>
</#list>

