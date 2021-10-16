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

import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.sql.Types;
import java.util.List;

/**
 * Extracts the time part of the time or datetime expression expr and returns it
 * as a string. This function is unsafe for statement-based replication. In
 * MySQL 5.6, a warning is logged if you use this function when binlog_format is
 * set to STATEMENT. (Bug #47995)
 *
 * <pre>
 * mysql> SELECT TIME('2003-12-31 01:02:03');
 *         -> '01:02:03'
 * mysql> SELECT TIME('2003-12-31 01:02:03.000123');
 *         -> '01:02:03.000123'
 * </pre>
 *
 * @author jianghang 2014-4-16 下午11:20:47
 * @since 5.0.7
 */
public class Time extends AbstractScalarFunction {
    public Time(List<DataType> operandTypes, DataType resultType) {
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
        }
        int scale = resultType.getScale();
        t = MySQLTimeCalculator.roundDatetime(t, scale);

        return DataTypeUtil.fromMySQLDatetime(resultType, t, InternalTimeZone.DEFAULT_TIME_ZONE);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"TIME"};
    }
}
