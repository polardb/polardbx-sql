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

package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.handler.pl.inner.ColumnarSetConfigProcedure;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mockStatic;

public class ColumnarSetConfigProcedureTest {

    @Test
    public void paramTest() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(null);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(any(), anyList(), any())).thenReturn(new int[] {1});

            ColumnarSetConfigProcedure procedure = new ColumnarSetConfigProcedure();
            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_set_config(aaa)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(null, statement, null);
                Assert.fail();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_set_config(123, \"aaa\")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(null, statement, null);
                Assert.fail();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_set_config(\"123\", \"ewc\", \"aaa\")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(null, statement, null);
                Assert.fail();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.query(any(), anyMap(), any(), any()))
                .thenReturn(new ArrayList<ColumnarTableMappingRecord>());

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql(
                    "call polardbx.columnar_set_config(\"aaa\", \"bbb\", \"ccc\", \"kk\", \"vv\")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            try {
                procedure.execute(null, statement, null);
                Assert.fail();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            ColumnarTableMappingRecord record = new ColumnarTableMappingRecord();
            record.tableSchema = "aaa";
            record.tableName = "bbb";
            record.indexName = "ccc";
            record.tableId = 2;
            List<ColumnarTableMappingRecord> records = new ArrayList<>();
            records.add(record);
            record = new ColumnarTableMappingRecord();
            record.tableSchema = "aaa";
            record.tableName = "bbb";
            record.indexName = "cccd";
            record.tableId = 4;
            records.add(record);
            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.query(any(), anyMap(), any(), any()))
                .thenReturn(records);

            try {
                procedure.execute(null, statement, null);
                Assert.fail();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

        }

    }

    @Test
    public void callTest() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(null);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(any(), anyList(), any())).thenReturn(new int[] {1});

            ColumnarTableMappingRecord record = new ColumnarTableMappingRecord();
            record.tableSchema = "aaa";
            record.tableName = "bbb";
            record.indexName = "ccc";
            record.tableId = 2;
            List<ColumnarTableMappingRecord> records = new ArrayList<>();
            records.add(record);
            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.query(any(), anyMap(), any(), any()))
                .thenReturn(records);

            ColumnarSetConfigProcedure procedure = new ColumnarSetConfigProcedure();
            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_set_config(\"aaa\", \"bbb\")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            ArrayResultCursor cursor = new ArrayResultCursor("test");
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals(2, cursor.getReturnColumns().size());
            Assert.assertTrue("aaa".equalsIgnoreCase(cursor.getRows().get(0).getObject(0).toString()));
            Assert.assertTrue("bbb".equalsIgnoreCase(cursor.getRows().get(0).getObject(1).toString()));

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_set_config(2, \"aaa\", \"bbb\")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            cursor = new ArrayResultCursor("test");
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals(3, cursor.getReturnColumns().size());
            Assert.assertEquals(2L, cursor.getRows().get(0).getObject(0));
            Assert.assertTrue("aaa".equalsIgnoreCase(cursor.getRows().get(0).getObject(1).toString()));
            Assert.assertTrue("bbb".equalsIgnoreCase(cursor.getRows().get(0).getObject(2).toString()));

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql(
                    "call polardbx.columnar_set_config(\"aaa\", \"bbb\", \"ccc\", \"kk\", \"vv\")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);
            cursor = new ArrayResultCursor("test");
            procedure.execute(null, statement, cursor);
            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals(6, cursor.getReturnColumns().size());
            Assert.assertTrue("aaa".equalsIgnoreCase(cursor.getRows().get(0).getObject(0).toString()));
            Assert.assertTrue("bbb".equalsIgnoreCase(cursor.getRows().get(0).getObject(1).toString()));
            Assert.assertTrue("ccc".equalsIgnoreCase(cursor.getRows().get(0).getObject(2).toString()));
            Assert.assertEquals(2L, cursor.getRows().get(0).getObject(3));
            Assert.assertTrue("kk".equalsIgnoreCase(cursor.getRows().get(0).getObject(4).toString()));
            Assert.assertTrue("vv".equalsIgnoreCase(cursor.getRows().get(0).getObject(5).toString()));
        }

    }

}
