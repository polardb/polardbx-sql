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

import com.alibaba.polardbx.optimizer.context.EvaluationContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public abstract class AbstractVectorizedExpression implements VectorizedExpression {
    protected VectorizedExpression[] children;
    protected DataType<?> outputDataType;
    protected int outputIndex;

    public AbstractVectorizedExpression(DataType<?> outputDataType, int outputIndex, VectorizedExpression[] children) {
        this.children = children;
        this.outputDataType = outputDataType;
        this.outputIndex = outputIndex;
    }

    protected void evalChildren(EvaluationContext ctx) {
        if (children != null) {
            for (VectorizedExpression child : children) {
                child.eval(ctx);
            }
        }
    }

    @Override
    public VectorizedExpression[] getChildren() {
        return children;
    }

    @Override
    public DataType<?> getOutputDataType() {
        return outputDataType;
    }

    @Override
    public int getOutputIndex() {
        return outputIndex;
    }
}
