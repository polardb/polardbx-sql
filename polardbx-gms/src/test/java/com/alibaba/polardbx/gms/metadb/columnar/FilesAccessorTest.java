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

package com.alibaba.polardbx.gms.metadb.columnar;

import com.alibaba.polardbx.gms.metadb.table.FileInfoRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class FilesAccessorTest {

    @Test
    public void testSelect() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<FileInfoRecord> recordList = new ArrayList<>();
            recordList.add(new FileInfoRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(FileInfoRecord.class), Mockito.any())).thenReturn(recordList);
            FilesAccessor accessor = new FilesAccessor();
            List<FileInfoRecord> result = accessor.queryFileInfoByLogicalSchemaTable("schema", "table");
            Assert.assertEquals(1, result.size());

            result = accessor.queryFileInfoByLogicalSchemaTableTso("schema", "10", 123L);
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void testDelete() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger deleteCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> deleteCount.get());
            FilesAccessor accessor = new FilesAccessor();
            deleteCount.set(100);
            int count = accessor.deleteLimit("schema", "table", 100);
            Assert.assertEquals(100, count);
            deleteCount.set(1000);
            count = accessor.delete("schema", "table");
            Assert.assertEquals(1000, count);
        }
    }

    @Test
    public void testFileInfoRecord() throws Exception {
        FileInfoRecord record = new FileInfoRecord();
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString(anyString())).thenReturn("123");
        when(rs.getLong(anyString())).thenReturn(222L);
        when(rs.wasNull()).thenReturn(false);
        record.fill(rs);

        Assert.assertEquals(222L, record.fileId);
        Assert.assertEquals("123", record.fileName);
    }

    @Test
    public void testQueryColumnarSnapshotFilesByTsoAndTableId() {
        AtomicReference<String> querySql = new AtomicReference<>();
        FilesAccessor filesAccessor = new FilesAccessor();

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(any(), any(), any(), any())).thenAnswer(
                invocation -> {
                    querySql.set(invocation.getArgument(0));
                    return new ArrayList<>();
                }
            );
            filesAccessor.queryColumnarSnapshotFilesByTsoAndTableId(1L, "test", "test");
            System.out.println(querySql.get());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(any(), any(), any(), any()))
                .thenThrow(new RuntimeException("test_xxx"));
            try {
                filesAccessor.queryColumnarSnapshotFilesByTsoAndTableId(1L, "test", "test");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("test_xxx"));
            }
        }
    }

    @Test
    public void testQuerySnapshotWithChecksumByTsoAndTableId() {
        AtomicReference<String> querySql = new AtomicReference<>();
        FilesAccessor filesAccessor = new FilesAccessor();

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(any(), any(), any(), any())).thenAnswer(
                invocation -> {
                    querySql.set(invocation.getArgument(0));
                    return new ArrayList<>();
                }
            );
            filesAccessor.querySnapshotWithChecksumByTsoAndTableId(1L, "test", "test");
            System.out.println(querySql.get());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(any(), any(), any(), any()))
                .thenThrow(new RuntimeException("test_xxx"));
            try {
                filesAccessor.querySnapshotWithChecksumByTsoAndTableId(1L, "test", "test");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("test_xxx"));
            }
        }
    }

    @Test
    public void testDelete2() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger deleteCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> deleteCount.get());
            FilesAccessor accessor = new FilesAccessor();
            deleteCount.set(100);
            int count = accessor.deleteOrcMetaByCommitTso(1024L);
            Assert.assertEquals(100, count);
            deleteCount.set(1000);
            count = accessor.deleteThreeMetaByCommitTso(2048);
            Assert.assertEquals(1000, count);

            deleteCount.set(10);
            count = accessor.deleteCompactionFileByCommitTso("a", "b", 2048);
            Assert.assertEquals(10, count);

        }
    }

    @Test
    public void testUpdate() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger updateCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> updateCount.get());
            FilesAccessor accessor = new FilesAccessor();
            updateCount.set(100);
            int count = accessor.updateRemoveTsByTso(1024L);
            Assert.assertEquals(100, count);

            updateCount.set(10);
            count = accessor.updateCompactionRemoveTsByTso("a", "b", 2048);
            Assert.assertEquals(10, count);

        }
    }
}
