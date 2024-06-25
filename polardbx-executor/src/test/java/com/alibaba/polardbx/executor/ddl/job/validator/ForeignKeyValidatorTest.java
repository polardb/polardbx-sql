/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.job.validator;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ForeignKeyValidatorTest {

//    @Test
//    public void fkRefIndexTest() {
//        String tableName = "tableName";
//        String columnName = "columnName";
//        ForeignKeyData foreignKeyData = Mockito.spy(new ForeignKeyData());
//        foreignKeyData.refTableName = tableName;
//        foreignKeyData.refColumns = new ArrayList<String>() {{
//            add(columnName);
//        }};
//
//        ExecutionContext executionContext = Mockito.spy(new ExecutionContext());
//        SqlCreateTable sqlCreateTable = mock(SqlCreateTable.class);
//
//        SqlIndexDefinition idx1 = new SqlIndexDefinition(SqlParserPos.ZERO, false, null, null, null, null, null, null,
//            ImmutableList.of(
//                new SqlIndexColumnName(SqlParserPos.ZERO, new SqlIdentifier(columnName, SqlParserPos.ZERO), null,
//                    null)),
//            null, null, null, null, null, null, false, null, true);
//
//        List<Pair<SqlIdentifier, SqlIndexDefinition>> uniqueKeys = new ArrayList<>();
//        uniqueKeys.add(Pair.of(new SqlIdentifier(columnName, SqlParserPos.ZERO), idx1));
//
//        when(sqlCreateTable.getUniqueKeys()).thenReturn(uniqueKeys);
//        ForeignKeyValidator.validateAddReferredTableFkIndex(foreignKeyData, executionContext, tableName,
//            sqlCreateTable);
//    }

    @Test
    public void testGetCharsetCollationName() {
        SqlDataTypeSpec datatypeMock = mock(SqlDataTypeSpec.class);
        SqlColumnDeclaration defMock = mock(SqlColumnDeclaration.class);
        SqlCreateTable sqlCreateTableMock = mock(SqlCreateTable.class);

        when(defMock.getDataType()).thenReturn(datatypeMock);
        when(datatypeMock.getCharSetName()).thenReturn("utf8");
        when(datatypeMock.getCollationName()).thenReturn("utf8_bin");

        try (MockedStatic<CharsetName> mockedCharsetName = Mockito.mockStatic(CharsetName.class)) {
            try (MockedStatic<CollationName> mockedCollationName = Mockito.mockStatic(CollationName.class)) {
                try (MockedStatic<SqlTypeUtil> mockedSqlTypeUtil = Mockito.mockStatic(SqlTypeUtil.class)) {
                    CollationName collationNameMock = CollationName.UTF8_BIN;
                    CharsetName charsetNameMock = CharsetName.UTF8;

                    mockedCollationName.when(() -> CollationName.findCollationName("utf8_bin"))
                        .thenReturn(collationNameMock);
                    mockedCollationName.when(() -> CollationName.getCharsetOf(collationNameMock))
                        .thenReturn(charsetNameMock);

                    when(sqlCreateTableMock.getDefaultCharset()).thenReturn("default_charset");
                    when(sqlCreateTableMock.getDefaultCollation()).thenReturn("default_collation");

                    Pair<String, String> result =
                        ForeignKeyValidator.getCharsetCollationName(defMock, sqlCreateTableMock);

                    assertEquals("UTF8", result.getKey());
                    assertEquals("UTF8_BIN", result.getValue());

                    when(defMock.getNotNull()).thenReturn(SqlColumnDeclaration.ColumnNull.NULL);

                    RelDataType relDataTypeMock = mock(RelDataType.class);
                    when(datatypeMock.deriveType(any(RelDataTypeFactory.class), eq(true)))
                        .thenReturn(relDataTypeMock); // nullable为true时返回的模拟对象

                    mockedCollationName.when(() -> CollationName.getCharsetOf(collationNameMock))
                        .thenReturn(null);
                    mockedSqlTypeUtil.when(() -> SqlTypeUtil.inCharFamily(any(RelDataType.class)))
                        .thenReturn(true);

                    result =
                        ForeignKeyValidator.getCharsetCollationName(defMock, sqlCreateTableMock);
                    assertEquals("default_charset", result.getKey());
                    assertEquals("default_collation", result.getValue());

                    mockedSqlTypeUtil.when(() -> SqlTypeUtil.inCharFamily(any(RelDataType.class)))
                        .thenReturn(false);

                    result =
                        ForeignKeyValidator.getCharsetCollationName(defMock, sqlCreateTableMock);
                    assertNull(result);

                }
            }
        }
    }
}
