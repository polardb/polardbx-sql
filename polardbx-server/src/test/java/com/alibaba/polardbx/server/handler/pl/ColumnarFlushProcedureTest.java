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

import com.alibaba.polardbx.common.cdc.CdcDDLContext;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.handler.pl.inner.ColumnarFlushProcedure;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class ColumnarFlushProcedureTest {

    @Test
    public void callTest() {
        try (MockedStatic<CdcManagerHelper> cdcManagerHelperMockedStatic = mockStatic(CdcManagerHelper.class)) {
            CdcManagerHelper cdcManagerHelper = mock(CdcManagerHelper.class);
            cdcManagerHelperMockedStatic.when(CdcManagerHelper::getInstance).thenReturn(cdcManagerHelper);
            doAnswer(invocation -> {
                CdcDDLContext cdcDDLContext = invocation.getArgument(0);
                cdcDDLContext.setCommitTso(100L);
                return null;
            }).when(cdcManagerHelper).notifyDdlWithContext(any());

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_flush()",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            ArrayResultCursor cursor = new ArrayResultCursor("columnar_flush");

            ColumnarFlushProcedure procedure = new ColumnarFlushProcedure();
            // Execute inner procedure.
            procedure.execute(null, statement, cursor);

            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals(100L, cursor.getRows().get(0).getObject(0));
        }
    }

    @Test
    public void callParamsTest() {
        try (MockedStatic<CdcManagerHelper> cdcManagerHelperMockedStatic = mockStatic(CdcManagerHelper.class);
            final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            CdcManagerHelper cdcManagerHelper = mock(CdcManagerHelper.class);
            cdcManagerHelperMockedStatic.when(CdcManagerHelper::getInstance).thenReturn(cdcManagerHelper);
            AtomicReference<String> markSql = new AtomicReference<>();
            doAnswer(invocation -> {
                CdcDDLContext cdcDDLContext = invocation.getArgument(0);
                cdcDDLContext.setCommitTso(100L);
                markSql.set(cdcDDLContext.getDdlSql());
                return null;
            }).when(cdcManagerHelper).notifyDdlWithContext(any());

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_flush()",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            List<ColumnarTableMappingRecord> records = new ArrayList<>();

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(any(), any(), any(), any())).thenReturn(records);

            ArrayResultCursor cursor = new ArrayResultCursor("columnar_flush");

            ColumnarFlushProcedure procedure = new ColumnarFlushProcedure();
            // Execute inner procedure.
            procedure.execute(null, statement, cursor);

            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals(100L, cursor.getRows().get(0).getObject(0));
            Assert.assertTrue(markSql.get().equalsIgnoreCase("call polardbx.columnar_flush()"));

            ColumnarTableMappingRecord record = new ColumnarTableMappingRecord();
            record.tableId = 11L;
            record.status = ColumnarTableStatus.PUBLIC.name();
            records.add(record);

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_flush(11)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            cursor = new ArrayResultCursor("columnar_flush");
            procedure.execute(null, statement, cursor);

            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals(100L, cursor.getRows().get(0).getObject(0));
            Assert.assertTrue(markSql.get().equalsIgnoreCase("call polardbx.columnar_flush(11)"));

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_flush(\"a\", \"b\", \"c\")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            cursor = new ArrayResultCursor("columnar_flush");
            procedure.execute(null, statement, cursor);

            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals(100L, cursor.getRows().get(0).getObject(0));
            Assert.assertTrue(markSql.get().equalsIgnoreCase("call polardbx.columnar_flush(11)"));
        }
    }

    @Test
    public void callErrorTest() {
        try (MockedStatic<CdcManagerHelper> cdcManagerHelperMockedStatic = mockStatic(CdcManagerHelper.class)) {
            CdcManagerHelper cdcManagerHelper = mock(CdcManagerHelper.class);
            cdcManagerHelperMockedStatic.when(CdcManagerHelper::getInstance).thenReturn(cdcManagerHelper);
            doAnswer(invocation -> {
                CdcDDLContext cdcDDLContext = invocation.getArgument(0);
                cdcDDLContext.setCommitTso(null);
                return null;
            }).when(cdcManagerHelper).notifyDdlWithContext(any());

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_flush()",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            ArrayResultCursor cursor = new ArrayResultCursor("columnar_flush");

            ColumnarFlushProcedure procedure = new ColumnarFlushProcedure();
            // Execute inner procedure.
            try {
                procedure.execute(null, statement, cursor);
                Assert.fail("should throw exception");
            } catch (Exception ignored) {

            }

            doAnswer(invocation -> {
                CdcDDLContext cdcDDLContext = invocation.getArgument(0);
                cdcDDLContext.setCommitTso(0L);
                return null;
            }).when(cdcManagerHelper).notifyDdlWithContext(any());

            try {
                procedure.execute(null, statement, cursor);
                Assert.fail("should throw exception");
            } catch (Exception ignored) {

            }

            doAnswer(invocation -> {
                CdcDDLContext cdcDDLContext = invocation.getArgument(0);
                cdcDDLContext.setCommitTso(-1L);
                return null;
            }).when(cdcManagerHelper).notifyDdlWithContext(any());

            try {
                procedure.execute(null, statement, cursor);
                Assert.fail("should throw exception");
            } catch (Exception ignored) {

            }
        }

    }

    @Test
    public void callParamsErrorTest() {
        try (MockedStatic<CdcManagerHelper> cdcManagerHelperMockedStatic = mockStatic(CdcManagerHelper.class);
            final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            CdcManagerHelper cdcManagerHelper = mock(CdcManagerHelper.class);
            cdcManagerHelperMockedStatic.when(CdcManagerHelper::getInstance).thenReturn(cdcManagerHelper);
            doAnswer(invocation -> {
                CdcDDLContext cdcDDLContext = invocation.getArgument(0);
                cdcDDLContext.setCommitTso(100L);
                return null;
            }).when(cdcManagerHelper).notifyDdlWithContext(any());

            List<ColumnarTableMappingRecord> records = new ArrayList<>();

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(any(), any(), any(), any())).thenReturn(records);

            ArrayResultCursor cursor = new ArrayResultCursor("columnar_flush");

            ColumnarFlushProcedure procedure = new ColumnarFlushProcedure();
            // Execute inner procedure.

            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_flush('b')",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            try {
                procedure.execute(null, statement, cursor);
                Assert.fail("should throw exception");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_flush(\"b\")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            try {
                procedure.execute(null, statement, cursor);
                Assert.fail("should throw exception");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_flush(\"b\", \"c\")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            try {
                procedure.execute(null, statement, cursor);
                Assert.fail("should throw exception");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_flush(1.2)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            try {
                procedure.execute(null, statement, cursor);
                Assert.fail("should throw exception");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_flush(1,2)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            try {
                procedure.execute(null, statement, cursor);
                Assert.fail("should throw exception");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_flush(1)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            try {
                procedure.execute(null, statement, cursor);
                Assert.fail("should throw exception");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_flush(a,b,c)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            try {
                procedure.execute(null, statement, cursor);
                Assert.fail("should throw exception");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_flush('a','b','c')",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            try {
                procedure.execute(null, statement, cursor);
                Assert.fail("should throw exception");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_flush(\"a\",\"b\",\"c\")",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            try {
                procedure.execute(null, statement, cursor);
                Assert.fail("should throw exception");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

        }
    }
}
