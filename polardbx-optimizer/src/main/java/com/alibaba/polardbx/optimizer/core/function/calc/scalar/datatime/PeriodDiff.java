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

import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * Returns the number of months between periods P1 and P2. P1 and P2 should be
 * in the format YYMM or YYYYMM. Note that the period arguments P1 and P2 are
 * not date values.
 *
 * <pre>
 * mysql> SELECT PERIOD_DIFF(200802,200703);
 *         -> 11
 * </pre>
 *
 * @author jianghang 2014-4-17 上午10:10:23
 * @since 5.0.7
 */
public class PeriodDiff extends AbstractScalarFunction {
    public PeriodDiff(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    public static final String[] DATE_FORMATS = new String[] {"yyyyMM", "yyMM", "yyyy-MM", "yy-MM"};

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        Long period1, period2;
        try {
            period1 = DataTypes.LongType.convertFrom(args[0]);
            period2 = DataTypes.LongType.convertFrom(args[1]);
        } catch (Throwable t) {
            return null;
        }

        if (period1 == null || period2 == null) {
            return null;
        }

        long months1 = MySQLTimeConverter.periodToMonth(period1);
        long months2 = MySQLTimeConverter.periodToMonth(period2);

        return months1 - months2;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"PERIOD_DIFF"};
    }
}
