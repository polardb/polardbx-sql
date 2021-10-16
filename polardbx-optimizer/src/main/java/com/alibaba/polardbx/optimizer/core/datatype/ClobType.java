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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.convertor.ConvertorException;
import com.alibaba.polardbx.optimizer.core.expression.bean.NullValue;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@linkplain Clob}类型，使用StringType来代替
 *
 * @author jianghang 2014-1-21 下午5:47:12
 * @since 5.0.0
 */
public class ClobType extends AbstractDataType<Clob> {

    @Override
    public Class getDataClass() {
        return Clob.class;
    }

    @Override
    public Clob getMaxValue() {
        throw new NotSupportException("range query is not supported for clob type");
    }

    @Override
    public Clob getMinValue() {
        throw new NotSupportException("range query is not supported for clob type");

    }

    @Override
    public Calculator getCalculator() {
        throw new NotSupportException("clob类型不支持计算操作");
    }

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                return rs.getClob(index);
            }

            @Override
            public Object get(Row rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }
        };
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }

        Clob value = convertFrom(o1);
        Clob targetValue = convertFrom(o2);

        if (value == null) {
            return -1;
        }

        if (targetValue == null) {
            return 1;
        }
        return value.equals(targetValue) ? 0 : 1;

    }

    @Override
    public Clob convertFrom(Object value) {
        if (value == null || value instanceof NullValue) {
            return null;
        } else {
            if (value instanceof Clob) {
                return (Clob) value;
            }

            throw new ConvertorException("Unsupported convert: [" + value.getClass().getName() + ","
                + Clob.class.getName() + "]");

        }
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.CLOB;
    }

    @Override
    public String getStringSqlType() {
        return "CLOB";
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_BLOB;
    }

}
