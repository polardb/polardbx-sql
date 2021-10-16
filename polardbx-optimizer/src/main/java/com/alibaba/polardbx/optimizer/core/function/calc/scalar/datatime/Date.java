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

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.sql.Types;
import java.util.List;

/**
 * Extracts the date part of the date or datetime expression expr.
 *
 * <pre>
 * mysql> SELECT DATE('2003-12-31 01:02:03');
 *         -> '2003-12-31'
 * </pre>
 *
 * @author jianghang 2014-4-16 下午5:25:10
 * @since 5.0.7
 */
public class Date extends AbstractScalarFunction {
    public Date(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        MysqlDateTime t = DataTypeUtil.toMySQLDatetimeByFlags(
            args[0],
            Types.TIMESTAMP,
            TimeParserFlags.FLAG_TIME_FUZZY_DATE | TimeParserFlags.FLAG_TIME_NO_DATE_FRAC_WARN
        );

        return DataTypeUtil.fromMySQLDatetime(resultType, t, InternalTimeZone.DEFAULT_TIME_ZONE);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"DATE"};
    }
}
