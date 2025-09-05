package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(names = {"LIKE"}, argumentTypes = {"Char", "Char"}, argumentKinds = {Variable, Const})
public class LikeCharColCharConstVectorizedExpression extends LikeVarcharColCharConstVectorizedExpression {

    public LikeCharColCharConstVectorizedExpression(
        int outputIndex,
        VectorizedExpression[] children) {
        super(outputIndex, children);
    }

}
