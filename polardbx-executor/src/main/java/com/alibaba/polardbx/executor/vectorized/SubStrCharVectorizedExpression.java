package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(
    names = {"SUBSTRING", "SUBSTR"},
    argumentTypes = {"Char", "Long", "Long"},
    argumentKinds = {Variable, Const, Const}
)
public class SubStrCharVectorizedExpression extends SubStrVarcharVectorizedExpression {

    public SubStrCharVectorizedExpression(DataType<?> outputDataType,
                                          int outputIndex, VectorizedExpression[] children) {
        super(outputDataType, outputIndex, children);
    }

}
