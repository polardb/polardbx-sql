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

package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mockStatic;

public class ColumnarNodeStatusUtilsTest {

    @Test
    public void testGet() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
            final MockedStatic<CdcUtils> cdcUtilsMockedStatic = mockStatic(
                CdcUtils.class);) {

            List<ColumnarTableMappingRecord> recordList = new ArrayList<>();

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarTableMappingRecord.class), Mockito.any())).thenReturn(recordList);

            ColumnarNodeStatusUtils.ColumnarNodeStatusInfo columnarNodeStatusInfo =
                ColumnarNodeStatusUtils.getColumnarNodeStatus();
            Assert.assertEquals(ColumnarNodeStatusUtils.ColumnarNodeStatus.NORMAL,
                columnarNodeStatusInfo.getColumnarNodeStatus());

            recordList.add(new ColumnarTableMappingRecord());

            List<ColumnarCheckpointsRecord> checkpointsRecords = new ArrayList<>();

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarCheckpointsRecord.class), Mockito.any())).thenReturn(checkpointsRecords);

            ColumnarNodeStatusUtils.getLastCheckTimeMs().set(0L);

            columnarNodeStatusInfo = ColumnarNodeStatusUtils.getColumnarNodeStatus();
            Assert.assertEquals(ColumnarNodeStatusUtils.ColumnarNodeStatus.PAUSED,
                columnarNodeStatusInfo.getColumnarNodeStatus());
            Assert.assertEquals("No columnar checkpoint", columnarNodeStatusInfo.getInfo());

            columnarNodeStatusInfo = ColumnarNodeStatusUtils.getColumnarNodeStatus();
            Assert.assertEquals(ColumnarNodeStatusUtils.ColumnarNodeStatus.PAUSED,
                columnarNodeStatusInfo.getColumnarNodeStatus());
            Assert.assertEquals("No columnar checkpoint", columnarNodeStatusInfo.getInfo());

            ColumnarCheckpointsRecord checkpointsRecord = new ColumnarCheckpointsRecord();
            checkpointsRecord.binlogTso = 7245839713959936000L;
            checkpointsRecords.add(checkpointsRecord);

            cdcUtilsMockedStatic.when(CdcUtils::getCdcTsoAndDelay)
                .thenReturn(Pair.of(7245854813454336000L, 1000 * 1000L));

            ColumnarNodeStatusUtils.getLastCheckTimeMs().set(0L);

            columnarNodeStatusInfo = ColumnarNodeStatusUtils.getColumnarNodeStatus();
            Assert.assertEquals(ColumnarNodeStatusUtils.ColumnarNodeStatus.PAUSED,
                columnarNodeStatusInfo.getColumnarNodeStatus());
            Assert.assertEquals("CDC delay too long, > 10min", columnarNodeStatusInfo.getInfo());

            cdcUtilsMockedStatic.when(CdcUtils::getCdcTsoAndDelay)
                .thenReturn(Pair.of(7245854813454336000L, 100L));

            ColumnarNodeStatusUtils.getLastCheckTimeMs().set(0L);

            columnarNodeStatusInfo = ColumnarNodeStatusUtils.getColumnarNodeStatus();
            Assert.assertEquals(ColumnarNodeStatusUtils.ColumnarNodeStatus.PAUSED,
                columnarNodeStatusInfo.getColumnarNodeStatus());
            Assert.assertEquals("Columnar delay too long, > 10min", columnarNodeStatusInfo.getInfo());

            checkpointsRecord.binlogTso = 7245854805065728000L;

            ColumnarNodeStatusUtils.getLastCheckTimeMs().set(0L);
            columnarNodeStatusInfo = ColumnarNodeStatusUtils.getColumnarNodeStatus();
            Assert.assertEquals(ColumnarNodeStatusUtils.ColumnarNodeStatus.NORMAL,
                columnarNodeStatusInfo.getColumnarNodeStatus());

            cdcUtilsMockedStatic.when(CdcUtils::getCdcTsoAndDelay)
                .thenThrow(new RuntimeException("get cdc tso and delay error"));

            try {
                ColumnarNodeStatusUtils.getLastCheckTimeMs().set(0L);
                ColumnarNodeStatusUtils.getColumnarNodeStatus();
                Assert.fail("should throw exception");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("get cdc tso and delay error"));
            }
        }
    }
}
