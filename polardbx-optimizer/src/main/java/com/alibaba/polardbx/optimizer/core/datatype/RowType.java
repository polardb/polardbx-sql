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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Row;

import java.util.List;

public class RowType extends AbstractDataType<Row.RowValue> {

    private final List<DataType> dataTypes;

    public RowType(List<DataType> dataTypes) {
        this.dataTypes = dataTypes;
    }

    @Override
    public Class getDataClass() {
        return Row.RowValue.class;
    }

    @Override
    public ResultGetter getResultGetter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Row.RowValue getMaxValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Row.RowValue getMinValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Calculator getCalculator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getSqlType() {
        return UNDECIDED_SQL_TYPE;
    }

    @Override
    public String getStringSqlType() {
        return "ROW";
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }
        if ((o1 instanceof Row.RowValue) && (o2 instanceof Row.RowValue)) {

            List<Object> o1s = ((Row.RowValue) o1).getValues();
            List<Object> o2s = ((Row.RowValue) o2).getValues();
            if (o1s.size() == o2s.size() && o1s.size() == dataTypes.size()) {
                for (int i = 0; i < o1s.size(); i++) {
                    int ret = dataTypes.get(i).compare(o1s.get(i), o2s.get(i));
                    if (ret != 0) {
                        return ret;
                    }
                }
                return 0;
            } else {
                throw new UnsupportedOperationException();
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
