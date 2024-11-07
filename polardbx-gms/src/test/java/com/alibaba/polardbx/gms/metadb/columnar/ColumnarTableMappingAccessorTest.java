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

import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mockStatic;

public class ColumnarTableMappingAccessorTest {

    @Test
    public void testSelect() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<ColumnarTableMappingRecord> recordList = new ArrayList<>();
            recordList.add(new ColumnarTableMappingRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarTableMappingRecord.class), Mockito.any())).thenReturn(recordList);
            TableInfoManager infoManager = new TableInfoManager();
            boolean result = infoManager.haveColumnarTable("schema", "table");
            Assert.assertFalse(result);
            result = infoManager.haveColumnarTable("schema", "10");
            Assert.assertTrue(result);
            result = infoManager.haveColumnarTable("schema", "10.1");
            Assert.assertFalse(result);
            result = infoManager.haveColumnarTable("schema", "102716261927172172171270127017021270172");
            Assert.assertFalse(result);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarTableMappingRecord.class), Mockito.any())).thenReturn(new ArrayList<>());
            result = infoManager.haveColumnarTable("schema", "table");
            Assert.assertFalse(result);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarTableMappingRecord.class), Mockito.any())).thenReturn(recordList);

            ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
            List<ColumnarTableMappingRecord> res = accessor.queryPurgeTablesByTso(16L);
            Assert.assertEquals(1, res.size());

            res = accessor.queryPurgeTablesWhichHavePurgeFilesByTso(18L);
            Assert.assertEquals(1, res.size());

            res = accessor.queryPurgeTablesWhichHavePurgeFilesByTsoAndType(199L, "snapshot");
            Assert.assertEquals(1, res.size());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(10);
            int count =
                accessor.updateStatusAndLastVersionIdByTableIdAndStatus(10L, 10L, ColumnarTableStatus.DROP.name(),
                    ColumnarTableStatus.PURGE.name());
            Assert.assertEquals(10, count);
        }
    }
}
