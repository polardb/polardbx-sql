/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(names = {"expr_test_1", "Expr_tesT"}, argumentTypes = {"Long", "Integer"},
    argumentKinds = {Const, Variable})
public class TestVectorizedExpression extends AbstractVectorizedExpression {
    public TestVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.BlobType, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
    }
}

