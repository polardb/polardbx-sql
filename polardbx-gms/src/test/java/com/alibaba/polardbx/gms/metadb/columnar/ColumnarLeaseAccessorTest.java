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

import com.alibaba.polardbx.gms.metadb.table.ColumnarLeaseAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarLeaseRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mockStatic;

public class ColumnarLeaseAccessorTest {

    @Test
    public void testInsert() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger insertCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> insertCount.get());
            ColumnarLeaseAccessor accessor = new ColumnarLeaseAccessor();
            insertCount.set(1);
            long result = accessor.forceElectInsert("owner", 111L, 100L);
            Assert.assertEquals(0, result);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenThrow(new SQLIntegrityConstraintViolationException("test"));
            result = accessor.forceElectInsert("owner", 111L, 100L);
            Assert.assertEquals(-1, result);
        }
    }

    @Test
    public void testSelect() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<ColumnarLeaseRecord> recordList = new ArrayList<>();
            recordList.add(new ColumnarLeaseRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.any(),
                Mockito.eq(ColumnarLeaseRecord.class), Mockito.any())).thenReturn(recordList);
            ColumnarLeaseAccessor accessor = new ColumnarLeaseAccessor();
            List<ColumnarLeaseRecord> result = accessor.forceElectSelectForUpdate();
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void testUpdate() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger updateCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> updateCount.get());
            ColumnarLeaseAccessor accessor = new ColumnarLeaseAccessor();
            updateCount.set(1);
            accessor.forceElectUpdate("owner", 121L, 1024L);

            updateCount.set(0);
            try {
                accessor.forceElectUpdate("owner", 121L, 1024L);
                Assert.fail("should throw exception");
            } catch (Exception ignore) {

            }

        }
    }

}
