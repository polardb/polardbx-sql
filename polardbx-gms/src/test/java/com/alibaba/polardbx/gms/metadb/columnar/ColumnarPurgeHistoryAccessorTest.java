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

import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ColumnarPurgeHistoryAccessorTest {

    @Test
    public void testSelect() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<ColumnarPurgeHistoryRecord> recordList = new ArrayList<>();
            recordList.add(new ColumnarPurgeHistoryRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), any(),
                Mockito.eq(ColumnarPurgeHistoryRecord.class), Mockito.any())).thenReturn(recordList);
            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();

            List<ColumnarPurgeHistoryRecord> result = accessor.queryLastPurgeTso();
            Assert.assertEquals(1, result.size());

            result = accessor.queryPurgeRecordByTso(100L);
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void testUpdate() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger updateCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> updateCount.get());
            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();
            updateCount.set(100);
            int count = accessor.updateStatusByTso(ColumnarPurgeHistoryRecord.PurgeStatus.START, 100L);
            Assert.assertEquals(100, count);
            updateCount.set(1000);
            count = accessor.updateStatusByTso("schema", 200L);
            Assert.assertEquals(1000, count);
        }
    }

    @Test
    public void testInsert() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            ColumnarPurgeHistoryRecord record = new ColumnarPurgeHistoryRecord();

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyList(),
                Mockito.any())).thenReturn(new int[1]);
            ColumnarPurgeHistoryAccessor accessor = new ColumnarPurgeHistoryAccessor();
            int[] result = accessor.insert(ImmutableList.of(record));
            Assert.assertEquals(1, result.length);

        }
    }

    @Test
    public void testColumnarPurgeHistoryRecord() throws Exception {
        ColumnarPurgeHistoryRecord record = new ColumnarPurgeHistoryRecord();
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString(anyString())).thenReturn("123");
        when(rs.getLong(anyString())).thenReturn(222L);
        when(rs.getTimestamp(anyString())).thenReturn(null);
        record.fill(rs);

        Assert.assertEquals(222L, record.id);
        Assert.assertEquals("123", record.status);
        Assert.assertNull(record.createTime);
    }
}
