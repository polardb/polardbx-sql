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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.datatime;

import java.sql.Types;
import java.util.List;

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

/**
 * Returns the second for time, in the range 0 to 59.
 *
 * <pre>
 * mysql> SELECT SECOND('10:05:03');
 *         -> 3
 * </pre>
 *
 * @author jianghang 2014-4-16 下午6:41:08
 * @since 5.0.7
 */
public class Second extends AbstractScalarFunction {
    public Second(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        Object timeObj = args[0];
        MysqlDateTime t = DataTypeUtil.toMySQLDatetime(timeObj, Types.TIME);
        if (t == null) {
            return null;
        } else {
            return t.getSecond();
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"SECOND"};
    }
}
