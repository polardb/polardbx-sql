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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.row.Row;
import io.airlift.slice.Slice;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class EnumType extends AbstractDataType<String> {

    public EnumType(List<String> enums) {
        for (int i = 0; i < enums.size(); i++) {
            final int value = i + 1;
            final String key = enums.get(i);
            enumValues.put(key, value);
            enumIndexs.put(value, key);
        }
    }

    public EnumType(Map<String, Integer> enumValues) {
        this.enumValues = enumValues;
        for (Map.Entry<String, Integer> entry : enumValues.entrySet()) {
            enumIndexs.put(entry.getValue(), entry.getKey());
        }
    }

    private Map<String, Integer> enumValues = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private Map<Integer, String> enumIndexs = new TreeMap<>();
    private final Calculator calculator = new AbstractDecimalCalculator() {
        @Override
        public Decimal convertToDecimal(Object v) {
            return DataTypes.DecimalType.convertFrom(v);
        }
    };

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
        return new String(new char[] {Character.MAX_VALUE});
    }

    @Override
    public String getMinValue() {
        return null; // 返回null值
    }

    public Object convertTo(DataType toType, EnumValue value) {
        if (toType instanceof NumberType) {
            return toType.convertFrom(this.enumValues.get(value.value));
        }

        return toType.convertFrom(value.value);
    }

    @Override
    public String convertFrom(Object value) {
        // for empty
        if (value == null) {
            return null;
        }

        String result = null;
        if (value instanceof Slice) {
            // converting from slice, is equal to convert from a string with utf-8 encoding.
            result = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
            return checkResult(result);
        }

        final Class<?> clazz = value.getClass();
        if (clazz == String.class) {
            result = (String) value;
            return checkResult(result);
        } else if (value instanceof Number) {
            return this.convertTo(((Number) value).intValue());
        } else if (clazz == EnumValue.class) {
            return ((EnumValue) value).value;
        }

        return null;
    }

    private String checkResult(String value) {
        if (!InstConfUtil.getBool(ConnectionParams.STRICT_ENUM_CONVERT)) {
            return value;
        }
        if (value != null && enumValues.containsKey(value)) {
            return value;
        } else {
            return null;
        }
    }

    public String convertTo(Integer value) {
        return enumIndexs.get(value);

    }

    public Map<String, Integer> getEnumValues() {
        return enumValues;
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }

        String no1 = convertFrom(o1);
        String no2 = convertFrom(o2);

        if (no1 == null) {
            return -1;
        }

        if (no2 == null) {
            return 1;
        }

        /**
         * mysql 默认不区分大小写，这里可能是个坑
         */
        return CaseInsensitive.compareToIgnoreCase(no1, no2);
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.VARCHAR;
    }

    @Override
    public String getStringSqlType() {
        return "VARCHAR";
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_ENUM;
    }

    @Override
    public Class getDataClass() {
        return Enum.class;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EnumType enumType = (EnumType) o;
        return Objects.equals(getEnumValues(), enumType.getEnumValues()) &&
            Objects.equals(enumIndexs, enumType.enumIndexs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getEnumValues(), enumIndexs);
    }

}
