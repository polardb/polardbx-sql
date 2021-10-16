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
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.time.ZonedDateTime;
import java.util.List;

/**
 * @author jianghang 2014-4-16 下午10:28:22
 * @since 5.0.7
 */
public class UtcDate extends AbstractScalarFunction {
    public UtcDate(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        // get zoned now datetime
        ZonedDateTime zonedDateTime = ZonedDateTime.now(InternalTimeZone.UTC_ZONE_ID);

        // round to scale.
        MysqlDateTime t = MySQLTimeTypeUtil.fromZonedDatetime(zonedDateTime);

        t = MySQLTimeConverter.dateTimeToDate(t);

        return DataTypeUtil.fromMySQLDatetime(resultType, t, InternalTimeZone.UTC_TIME_ZONE);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"UTC_DATE"};
    }
}
