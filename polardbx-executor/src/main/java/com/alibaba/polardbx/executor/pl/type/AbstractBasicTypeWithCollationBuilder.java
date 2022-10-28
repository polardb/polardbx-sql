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

package com.alibaba.polardbx.executor.pl.type;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCharacterDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.BasicSqlType;

import java.nio.charset.Charset;
import java.util.Optional;

public abstract class AbstractBasicTypeWithCollationBuilder extends AbstractBasicSqlTypeBuilder {

    @Override
    public RelDataType createBasicSqlType(RelDataTypeSystem typeSystem,
                                          SQLDataType dataType) {
        BasicSqlType type = (BasicSqlType) super.createBasicSqlType(typeSystem, dataType);
        if (isExistCollation(dataType)) {
            return addCharsetAndCollation(dataType, type);
        } else if (isExistCharset(dataType)) {
            return addCharset(dataType, type);
        } else {
            return addDefaultCharset(type);
        }
    }

    private RelDataType addDefaultCharset(BasicSqlType type) throws RuntimeException {
        CharsetName charsetName = CharsetName.defaultCharset();
        Charset charset = Charset.forName(charsetName.name());
        CollationName collationName = charsetName.getDefaultCollationName();
        SqlCollation sqlCollation =
            new SqlCollation(charset, collationName.name(), SqlCollation.Coercibility.COERCIBLE);
        return type.createWithCharsetAndCollation(charset, sqlCollation);
    }

    private RelDataType addCharsetAndCollation(SQLDataType dataType, BasicSqlType type) throws RuntimeException {
        String collation = getCollationName(dataType);
        CollationName collationName = Optional.ofNullable(CollationName.of(collation)).orElseThrow(
            () -> new RuntimeException(String.format("collation: %s not support", collation)));
        Charset charset = Optional.ofNullable(CollationName.getCharsetOf(collationName)).map(CharsetName::toJavaCharset)
            .orElseThrow(() -> new RuntimeException(
                String.format("collation: %s not support, we can't find it's charset", collation)));
        SqlCollation sqlCollation =
            new SqlCollation(charset, collationName.name(), SqlCollation.Coercibility.COERCIBLE);
        return type.createWithCharsetAndCollation(charset, sqlCollation);
    }

    private RelDataType addCharset(SQLDataType dataType, BasicSqlType type) throws RuntimeException {
        String mysqlCharName = getCharsetName(dataType);
        CharsetName charsetName = Optional.ofNullable(mysqlCharName).map(CharsetName::of).orElseGet(
            CharsetName::defaultCharset);
        Charset charset = CharsetName.convertStrToJavaCharset(mysqlCharName);
        CollationName collationName = charsetName.getDefaultCollationName();
        SqlCollation sqlCollation =
            new SqlCollation(charset, collationName.name(), SqlCollation.Coercibility.COERCIBLE);
        return type.createWithCharsetAndCollation(charset, sqlCollation);
    }

    private boolean isExistCharset(SQLDataType dataType) {
        return dataType instanceof SQLCharacterDataType && !TStringUtil.isEmpty(
            ((SQLCharacterDataType) dataType).getCharSetName());
    }

    private boolean isExistCollation(SQLDataType dataType) {
        return dataType instanceof SQLCharacterDataType && !TStringUtil.isEmpty(
            ((SQLCharacterDataType) dataType).getCollate());
    }

    private String getCollationName(SQLDataType dataType) {
        if (dataType instanceof SQLCharacterDataType) {
            return ((SQLCharacterDataType) dataType).getCollate();
        } else {
            return null;
        }
    }

    private String getCharsetName(SQLDataType dataType) {
        if (dataType instanceof SQLCharacterDataType) {
            return ((SQLCharacterDataType) dataType).getCharSetName();
        } else {
            return null;
        }
    }
}
