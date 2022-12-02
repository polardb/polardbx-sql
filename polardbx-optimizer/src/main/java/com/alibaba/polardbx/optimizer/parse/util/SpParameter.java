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

package com.alibaba.polardbx.optimizer.parse.util;

import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import io.airlift.slice.Slice;

public class SpParameter {
    Object currentValue;
    SQLDataType sqlDataType;

    public SpParameter(SQLDataType sqlDataType, Object currentValue) {
        this.sqlDataType = sqlDataType;
        this.currentValue = replaceSlice(currentValue);
    }

    public Object getCurrentValue() {
        return currentValue;
    }

    // TODO in mysql, if parameter's value beyond the scope of its type, procedure will terminate,
    // TODO and error will occur instead of implicit case
    public void setValue(Object value) {
        this.currentValue = replaceSlice(value);
    }

    private Object replaceSlice(Object value) {
        if (value instanceof Slice) {
            value = ((Slice) value).toStringUtf8();
        }
        return value;
    }
}
