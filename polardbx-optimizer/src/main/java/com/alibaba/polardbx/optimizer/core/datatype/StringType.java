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
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.charset.CharsetFactory;
import com.alibaba.polardbx.common.charset.CharsetHandler;
import com.alibaba.polardbx.common.collation.CollationHandler;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class StringType extends AbstractDataType<String> {

    private final CharsetName charsetName;
    private final CollationName collationName;
    private final CharsetHandler charsetHandler;
    private final CollationHandler collationHandler;
    private final boolean isUtf8Encoding;

    public StringType() {
        this(CharsetName.defaultCharset(), CollationName.defaultCollation());
    }

    public StringType(CharsetName charsetName, CollationName collationName) {
        this.collationName = collationName == null ? CollationName.defaultCollation() : collationName;
        // fix charset and collation info
        this.charsetName = Optional.ofNullable(collationName)
            .map(CollationName::getCharsetOf)
            .orElse(CharsetName.defaultCharset());

        this.charsetHandler = CharsetFactory.INSTANCE.createCharsetHandler(this.charsetName, this.collationName);
        this.collationHandler = charsetHandler.getCollationHandler();
        this.isUtf8Encoding = charsetName == CharsetName.UTF8MB4 || charsetName == CharsetName.UTF8;
    }

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

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }

        // follow MySQL behavior
        if (o1 instanceof Number || o2 instanceof Number) {
            return DataTypes.DecimalType.compare(o1, o2);
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
    public Class getDataClass() {
        return String.class;
    }

    public CharsetHandler getCharsetHandler() {
        return charsetHandler;
    }

    public CollationHandler getCollationHandler() {
        return collationHandler;
    }

    @Override
    public CharsetName getCharsetName() {
        return charsetName;
    }

    @Override
    public CollationName getCollationName() {
        return collationName;
    }

    @Override
    public boolean isUtf8Encoding() {
        return isUtf8Encoding;
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_VAR_STRING;
    }
}
