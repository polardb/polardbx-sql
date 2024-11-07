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

package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.google.common.base.Preconditions;

/**
 * @author fangwu
 */
public abstract class BaseColumnIndex implements ColumnIndex {
    private long rgNum;

    protected BaseColumnIndex(long rgNum) {
        this.rgNum = rgNum;
    }

    @Override
    public long rgNum() {
        return rgNum;
    }

    /**
     * @param value: input data type
     * @param dt: column data type
     * @param clazz: excepted input data type
     */
    protected <T> T paramTransform(Object value, DataType dt, Class<T> clazz) {
        Preconditions.checkArgument(value != null && dt != null, "value and DataType can't be null here");
        if (DataTypeUtil.isIntType(dt)) {
            if (clazz.equals(Long.class) && value instanceof Number) {
                return clazz.cast(((Number) value).longValue());
            }
        } else if (DataTypeUtil.isStringSqlType(dt)) {
            if (clazz.equals(String.class) && value instanceof String) {
                return clazz.cast(value);
            }
        } else if (DataTypeUtil.isDateType(dt)) {
            MysqlDateTime date = DataTypeUtil.toMySQLDatetime(value, dt.getSqlType());
            if (clazz.equals(Long.class) && date != null) {
                return clazz.cast(date.toPackedLong());
            }
        }
        return null;
    }
}
