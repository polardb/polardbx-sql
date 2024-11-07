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
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.handler.pl.inner.ColumnarRollbackProcedure;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ColumnarRollbackProcedureTest {

    @Test
    public void paramTest() {
        ArrayResultCursor cursor = new ArrayResultCursor("test");
        ServerConnection connection = Mockito.mock(ServerConnection.class);
        when(connection.getHost()).thenReturn("127.0.0.1");
        when(connection.getUser()).thenReturn("user_one");
        ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();
        SQLCallStatement statement =
            (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(10)",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(connection, statement, cursor);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        when(connection.getUser()).thenReturn("polardbx_root");

        statement =
            (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback()",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(connection, statement, cursor);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        statement =
            (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(\"123\")",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(connection, statement, cursor);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        statement =
            (SQLCallStatement) FastsqlUtils.parseSql(
                "call polardbx.columnar_rollback(-120)",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(connection, statement, cursor);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        statement =
            (SQLCallStatement) FastsqlUtils.parseSql(
                "call polardbx.columnar_rollback(120.1)",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(connection, statement, cursor);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        statement =
            (SQLCallStatement) FastsqlUtils.parseSql(
                "call polardbx.columnar_rollback(120, 30)",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(connection, statement, cursor);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    @Test
    public void executeTest() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ServerConnection connection = Mockito.mock(ServerConnection.class);
            when(connection.getHost()).thenReturn("127.0.0.1");
            when(connection.getUser()).thenReturn("polardbx_root");
            ColumnarRollbackProcedure procedure = new ColumnarRollbackProcedure();
            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_rollback(1234569)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.eq(ColumnarCheckpointsRecord.class), Mockito.any())).thenReturn(new ArrayList<>());
            try {
                procedure.execute(connection, statement, cursor);
                Assert.fail();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            List<ColumnarCheckpointsRecord> checkpointsRecords = new ArrayList<>();
            ColumnarCheckpointsRecord record = new ColumnarCheckpointsRecord();
            record.setCheckpointTso(1234569L);
            checkpointsRecords.add(record);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.eq(ColumnarCheckpointsRecord.class), Mockito.any())).thenReturn(checkpointsRecords);

            List<ColumnarPurgeHistoryRecord> purgeRecords = new ArrayList<>();
            ColumnarPurgeHistoryRecord record1 = new ColumnarPurgeHistoryRecord();
            record1.tso = 1234569L + 10;
            purgeRecords.add(record1);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.eq(ColumnarPurgeHistoryRecord.class), Mockito.any())).thenReturn(purgeRecords);

            try {
                procedure.execute(connection, statement, cursor);
                Assert.fail();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            Connection connection2 = Mockito.mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(connection2);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.eq(ColumnarPurgeHistoryRecord.class), Mockito.any())).thenReturn(new ArrayList<>());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);

            procedure.execute(connection, statement, cursor);
        }

    }
}
