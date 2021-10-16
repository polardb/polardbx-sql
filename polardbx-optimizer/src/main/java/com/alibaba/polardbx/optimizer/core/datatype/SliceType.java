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
import com.alibaba.polardbx.common.charset.MySQLUnicodeUtils;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.charset.CharsetFactory;
import com.alibaba.polardbx.common.charset.CharsetHandler;
import com.alibaba.polardbx.common.collation.CollationHandler;
import com.alibaba.polardbx.optimizer.core.expression.bean.NullValue;
import com.alibaba.polardbx.optimizer.core.row.Row;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class SliceType extends AbstractDataType<Slice> {
    // todo length to be a system config parameter
    private static final int MAX_CHAR_LENGTH = 1024;
    private static final int DEFAULT_FIELD_LEN = 1024;

    private final CharsetName charsetName;
    private final CollationName collationName;
    private final CharsetHandler charsetHandler;
    private final CollationHandler collationHandler;
    private final boolean isUtf8Encoding;
    private final boolean isLatin1Encoding;
    private final boolean isBinaryCollation;

    private int length;

    public SliceType() {
        this(CharsetName.defaultCharset(), CollationName.defaultCollation());
    }

    public SliceType(CollationName collationName) {
        this(null, collationName);
    }

    public SliceType(CharsetName charsetName, CollationName collationName) {
        this(charsetName, collationName, DEFAULT_FIELD_LEN);
    }

    public SliceType(int precision) {
        this(CharsetName.defaultCharset(), CollationName.defaultCollation(), precision);
    }

    public SliceType(CollationName collationName, int precision) {
        this(null, collationName, precision);
    }

    public SliceType(CharsetName charsetName, CollationName collationName, int precision) {
        this.collationName = collationName == null ? CollationName.defaultCollation() : collationName;
        // fix charset and collation info
        this.charsetName = Optional.ofNullable(collationName)
            .map(CollationName::getCharsetOf)
            .orElse(CharsetName.defaultCharset());

        this.charsetHandler = CharsetFactory.INSTANCE.createCharsetHandler(this.charsetName, this.collationName);
        this.collationHandler = charsetHandler.getCollationHandler();
        this.isUtf8Encoding = this.charsetName == CharsetName.UTF8MB4 || this.charsetName == CharsetName.UTF8;
        this.isLatin1Encoding = this.charsetName == CharsetName.LATIN1;
        this.isBinaryCollation =
            this.collationName == CollationName.BINARY || this.collationName.name().endsWith("_BIN");

        this.length = precision * charsetHandler.maxLenOfMultiBytes();
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
                if (isUtf8Encoding) {
                    byte[] utf8Bytes = rs.getBytes(index);
                    return Slices.wrappedBuffer(utf8Bytes);
                } else if (isLatin1Encoding) {
                    byte[] latin1Bytes = rs.getBytes(index);
                    return MySQLUnicodeUtils.latin1ToUtf8(latin1Bytes);
                } else {
                    String val = rs.getString(index);
                    return convertFrom(val);
                }
            }

            @Override
            public Object get(Row rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }
        };
    }

    @Override
    public Slice getMaxValue() {
        return Slices.wrappedBuffer(new byte[] {Byte.MAX_VALUE});
    }

    @Override
    public Slice getMinValue() {
        return Slices.wrappedBuffer(new byte[] {Byte.MIN_VALUE});
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

        // only if one of the object is sort key, compare by sort key object.
        if (o1 instanceof SortKey || o2 instanceof SortKey) {
            SortKey sortKey1 = makeSortKey(o1);
            SortKey sortKey2 = makeSortKey(o2);
            if (sortKey1 == null) {
                return -1;
            }

            if (sortKey2 == null) {
                return 1;
            }
            return sortKey1.compareTo(sortKey2);
        }

        // no1, no2 are all utf-8 encoding strings.
        Slice no1 = convertFrom(o1);
        Slice no2 = convertFrom(o2);

        if (no1 == null) {
            return -1;
        }

        if (no2 == null) {
            return 1;
        }

        // optimization for latin1 collation
        if ((isLatin1Encoding && !isBinaryCollation) || isUtf8Encoding) {
            return collationHandler.compareSp(no1, no2);
        }

        // utf-8 strings to special encoding strings.
        Slice binaryStr1 = charsetHandler.encodeFromUtf8(no1);
        Slice binaryStr2 = charsetHandler.encodeFromUtf8(no2);
        return collationHandler.compareSp(binaryStr1, binaryStr2);

    }

    public int instr(Object o1, Object o2) {
        Slice no1 = convertFrom(o1);
        Slice no2 = convertFrom(o2);

        Slice binaryStr1 = charsetHandler.encodeFromUtf8(no1);
        Slice binaryStr2 = charsetHandler.encodeFromUtf8(no2);

        return collationHandler.instr(binaryStr1, binaryStr2);
    }

    public SortKey makeSortKey(Object o) {
        if (o instanceof SortKey) {
            return (SortKey) o;
        } else {
            return Optional.ofNullable(o)
                .map(this::convertFrom)
                .map(this::getSortKey)
                .orElse(null);
        }
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
        return Slice.class;
    }

    public CharsetHandler getCharsetHandler() {
        return charsetHandler;
    }

    public CollationHandler getCollationHandler() {
        return collationHandler;
    }

    public CharsetName getCharsetName() {
        return charsetName;
    }

    public CollationName getCollationName() {
        return collationName;
    }

    public int hashcode(Slice s) {
        // optimization for latin1 collation
        if ((isLatin1Encoding && !isBinaryCollation) || isUtf8Encoding) {
            return collationHandler.hashcode(s);
        }
        Slice str = charsetHandler.encodeFromUtf8(s);
        return collationHandler.hashcode(str);
    }

    @Override
    public Slice convertFrom(Object value) {
        if (value instanceof Slice) {
            return (Slice) value;
        } else {
            // non-slice value should be firstly converted to string.
            return Optional.ofNullable(value)
                .map(DataTypes.StringType::convertFrom)
                .map(str -> str.getBytes(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK))
                .map(Slices::wrappedBuffer)
                .orElse(null);
        }
    }

    @Override
    public Object convertJavaFrom(Object value) {
        if (value == null || value instanceof NullValue) {
            return null;
        }
        if (value instanceof String) {
            return value;
        }
        return DataTypes.StringType.convertFrom(value);
    }

    @Override
    public boolean isUtf8Encoding() {
        return isUtf8Encoding;
    }

    @Override
    public boolean isLatin1Encoding() {
        return isLatin1Encoding;
    }

    public SortKey getSortKey(Slice slice) {
        // optimization for latin1 collation
        if ((isLatin1Encoding && !isBinaryCollation) || isUtf8Encoding) {
            return collationHandler.getSortKey(slice, MAX_CHAR_LENGTH);
        }
        // for other collations.
        Slice rawBytes = charsetHandler.encodeFromUtf8(slice);
        return collationHandler.getSortKey(rawBytes, MAX_CHAR_LENGTH);
    }

    public void setLength(int precision) {
        this.length = precision * charsetHandler.maxLenOfMultiBytes();
    }

    @Override
    public int length() {
        return length == 0 ? DEFAULT_FIELD_LEN : length;
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_VAR_STRING;
    }
}
