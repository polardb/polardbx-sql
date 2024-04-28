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

package com.alibaba.polardbx.optimizer.core.expression;

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.IFunction.FunctionType;
import com.alibaba.polardbx.optimizer.core.expression.bean.FunctionSignature;

import java.util.List;
import java.util.Map;

/**
 * 扩展函数实例，比如用于实现Merge的count/min等聚合函数
 *
 * @since 5.0.0
 */
public interface IExtraFunction {

    /**
     * 一个不可计算的对象
     */
    public Object UNEVALUATABLE = new Object();

    /**
     * 设置function配置定义
     */
    public void setFunction(IFunction function);

    /**
     * Aggregate/Scalar函数
     */
    public FunctionType getFunctionType();

    /**
     * 提前计算函数
     */
    public Object compute();

    /**
     * 根据函数正常的输入参数, 计算函数结果并返回
     *
     * @param funArgs 函数正常的输入参数
     * @param extraParams 函数正常执行的一些额外参数，用于扩展，加针对特殊HINT加的特殊处理等
     * @return 返回结果
     */
    public Object computeWithArgs(Object[] funArgs, Map<String, Object> extraParams);

    /**
     * 获取最后返回结果类型
     */
    public abstract DataType getReturnType();

    public Object evaluateResult(Object[] args, ExecutionContext ec);

    public String[] getFunctionNames();

    public void clear();

    public int getScale();

    public int getPrecision();

    public void setResultField(Field field);

    public FunctionSignature[] getFunctionSignature();

    default void setOperandFields(List<Field> fields) {
    }

    default void setCollation(CollationName collation) {
    }
}
