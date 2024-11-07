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
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.FileInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.handler.pl.inner.ColumnarSnapshotFilesProcedure;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mockStatic;

public class ColumnarSnapshotFilesProcedureTest {

    @Test
    public void paramTest() {
        ArrayResultCursor cursor = new ArrayResultCursor("test");
        ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
        SQLCallStatement statement =
            (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_snapshot_files()",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(null, statement, cursor);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        statement =
            (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_snapshot_files(12.1)",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(null, statement, cursor);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        statement =
            (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_snapshot_files(\"123\")",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(null, statement, cursor);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        statement =
            (SQLCallStatement) FastsqlUtils.parseSql(
                "call polardbx.columnar_snapshot_files(-120)",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(null, statement, cursor);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        statement =
            (SQLCallStatement) FastsqlUtils.parseSql(
                "call polardbx.columnar_snapshot_files(120, 30)",
                SQLParserFeature.IgnoreNameQuotes).get(0);
        try {
            procedure.execute(null, statement, cursor);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    @Test
    public void executeTest() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            ArrayResultCursor cursor = new ArrayResultCursor("test");
            ColumnarSnapshotFilesProcedure procedure = new ColumnarSnapshotFilesProcedure();
            SQLCallStatement statement =
                (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.columnar_snapshot_files(77665544)",
                    SQLParserFeature.IgnoreNameQuotes).get(0);

            List<ColumnarCheckpointsRecord> records = new ArrayList<>();
            records.add(new ColumnarCheckpointsRecord());
            records.get(0).setBinlogTso(77665544L);
            records.get(0).setCheckpointTso(77665544L);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.eq(ColumnarCheckpointsRecord.class), Mockito.any())).thenReturn(records);

            List<FileInfoRecord> fileInfoRecords = new ArrayList<>();
            FileInfoRecord fileInfoRecord = new FileInfoRecord();
            fileInfoRecord.setFileName("a.orc");
            fileInfoRecord.setExtentSize(1024L);
            fileInfoRecords.add(fileInfoRecord);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.eq(FileInfoRecord.class), Mockito.any())).thenReturn(fileInfoRecords);

            List<ColumnarAppendedFilesRecord> appendedFilesRecords = new ArrayList<>();
            ColumnarAppendedFilesRecord appendedFilesRecord = new ColumnarAppendedFilesRecord();
            appendedFilesRecord.setFileName("b.csv");
            appendedFilesRecord.setAppendOffset(2048L);
            appendedFilesRecord.setAppendLength(4096L);
            appendedFilesRecords.add(appendedFilesRecord);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.eq(ColumnarAppendedFilesRecord.class), Mockito.any())).thenReturn(appendedFilesRecords);

            procedure.execute(null, statement, cursor);
            System.out.println(cursor.getRows());
        }
    }
}
