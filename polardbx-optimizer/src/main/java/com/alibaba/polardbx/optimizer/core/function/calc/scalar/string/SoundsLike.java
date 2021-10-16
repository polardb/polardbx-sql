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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.string;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractCollationScalarFunction;

import java.util.List;

/**
 * 不支持
 *
 * @author mengshi.sunmengshi 2014年4月15日 下午5:44:59
 * @since 5.1.0
 */
public class SoundsLike extends AbstractCollationScalarFunction {
    public SoundsLike(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"SOUNDS LIKE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        throw new UnsupportedOperationException("如果没法下推，sounds like不支持");
    }
}
