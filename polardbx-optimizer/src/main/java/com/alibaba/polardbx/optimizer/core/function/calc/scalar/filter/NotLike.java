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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BooleanType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.List;

/**
 * not like func
 *
 * @author jianghang 2014-7-25 下午10:49:45
 * @since 5.1.8
 */
public class NotLike extends Like {
    public NotLike(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object flag = super.compute(args, ec);
        if (flag == null) {
            return null;
        } else {
            Boolean f = BooleanType.isTrue(DataTypes.BooleanType.convertFrom(flag));
            return !f ? 1L : 0L;
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"NOT LIKE"};
    }
}
