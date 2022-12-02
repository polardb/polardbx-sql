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
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.sql.Types;

public class VarcharType extends SliceType {
    public static final VarcharType DEFAULT_COLLATION_VARCHAR_TYPE = new VarcharType();

    public VarcharType() {
        super();
    }

    public VarcharType(CollationName collationName) {
        super(collationName);
    }

    public VarcharType(CharsetName charsetName, CollationName collationName) {
        super(charsetName, collationName);
    }

    public VarcharType(int precision) {
        super(precision);
    }

    public VarcharType(CollationName collationName, int precision) {
        super(collationName, precision);
    }

    public VarcharType(CharsetName charsetName, CollationName collationName, int precision) {
        super(charsetName, collationName, precision);
    }

    @Override
    public int getSqlType() {
        return Types.VARCHAR;
    }

    @Override
    public String getStringSqlType() {
        return "VARCHAR";
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_VAR_STRING;
    }

    @Override
    public Slice convertFrom(Object value) {
        if (getCharsetName() == CharsetName.BINARY && value instanceof byte[]) {
            return Slices.wrappedBuffer((byte[]) value);
        } else {
            return super.convertFrom(value);
        }
    }
}
