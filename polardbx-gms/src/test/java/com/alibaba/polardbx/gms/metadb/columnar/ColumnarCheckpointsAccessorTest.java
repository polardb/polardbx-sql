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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mockStatic;

public class ColumnarCheckpointsAccessorTest {

    @Test
    public void testDelete() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger deleteCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> deleteCount.get());
            ColumnarCheckpointsAccessor accessor = new ColumnarCheckpointsAccessor();
            deleteCount.set(100);
            int count = accessor.deleteLimit("schema", "table", 100);
            Assert.assertEquals(100, count);
            deleteCount.set(1000);
            count = accessor.deleteLimitByTsoAndTypes(12345, ImmutableList.of(
                ColumnarCheckpointsAccessor.CheckPointType.STREAM), 1000);
            Assert.assertEquals(1000, count);

            deleteCount.set(10);
            count = accessor.deleteByTso(1222L);
            Assert.assertEquals(10, count);

            deleteCount.set(16);
            count = accessor.deleteCompactionByTso(1222L);
            Assert.assertEquals(16, count);

            deleteCount.set(18);
            count = accessor.deleteLimitByTsoAndTypesAndInfoIsNull(12345, ImmutableList.of(
                ColumnarCheckpointsAccessor.CheckPointType.STREAM), 1000);
            Assert.assertEquals(18, count);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenThrow(new RuntimeException("metaDB error"));

            try {
                count = accessor.deleteLimitByTsoAndTypesAndInfoIsNull(12345, ImmutableList.of(
                    ColumnarCheckpointsAccessor.CheckPointType.STREAM), 1000);
                Assert.fail();
            } catch (Exception ignored) {

            }
        }
    }

    @Test
    public void testBuildInsertParam() {
        ColumnarCheckpointsRecord record = new ColumnarCheckpointsRecord();
        record.binlogTso = 12345L;
        record.setPartitionName("p1");
        record.setInfo("info");
        record.setMinCompactionTso(234L);
        record.offset = "offset";

        Map<Integer, ParameterContext> params = record.buildInsertParams();
        Assert.assertEquals(12345L, params.get(6).getValue());
        Assert.assertEquals("p1", params.get(3).getValue());
        Assert.assertEquals("info", params.get(9).getValue());
        Assert.assertEquals(234L, params.get(8).getValue());
    }

    @Test
    public void testSelect() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<ColumnarCheckpointsRecord> recordList = new ArrayList<>();
            recordList.add(new ColumnarCheckpointsRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarCheckpointsRecord.class), Mockito.any())).thenReturn(recordList);
            ColumnarCheckpointsAccessor accessor = new ColumnarCheckpointsAccessor();
            List<ColumnarCheckpointsRecord> result = accessor.queryColumnarTsoByBinlogTso(123L);
            Assert.assertEquals(1, result.size());

            result = accessor.queryCompactionByStartTsoAndEndTso(100L, 123L);
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void testSelectByTso() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<ColumnarCheckpointsRecord> recordList = new ArrayList<>();
            recordList.add(new ColumnarCheckpointsRecord());
            List<ColumnarTableEvolutionRecord> recordList1 = new ArrayList<>();
            recordList1.add(new ColumnarTableEvolutionRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarCheckpointsRecord.class), Mockito.any())).thenReturn(recordList);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarTableEvolutionRecord.class), Mockito.any())).thenReturn(recordList1);
            ColumnarCheckpointsAccessor accessor = new ColumnarCheckpointsAccessor();
            List<ColumnarCheckpointsRecord> result = accessor.queryByTso(123L);
            Assert.assertEquals(1, result.size());

            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.queryColumnarTableEvolutionByVersionId(123L);

            result = tableInfoManager.queryColumnarCheckpointsByCommitTs(123L);
            Assert.assertEquals(1, result.size());
        }
    }

}
