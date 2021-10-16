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
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author wuheng.zxy 2016-4-18 上午10:43:01
 */
public class JsonType extends AbstractDataType<String> {

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                return rs.getString(index);
            }

            @Override
            public Object get(Row rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }
        };
    }

    @Override
    public String getMaxValue() {
        throw new NotSupportException("json类型不支持直接Max操作");
    }

    @Override
    public String getMinValue() {
        throw new NotSupportException("json类型不支持直接min操作");
    }

    @Override
    public Calculator getCalculator() {
        throw new NotSupportException("json类型不支持直接计算");
    }

    @Override
    public int getSqlType() {
        return DataType.JSON_SQL_TYPE;
    }

    @Override
    public String getStringSqlType() {
        return "JSON";
    }

    @Override
    public int compare(Object o1, Object o2) {
        throw new NotSupportException("json类型不支持直接比较");
    }

    @Override
    public Class getDataClass() {
        return String.class;
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_JSON;
    }
}
