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

package com.alibaba.polardbx.gms.metadb;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ColumnarPartitionEvolutionAccessorTest {

    @Mock
    private Connection connection;

    @InjectMocks
    private ColumnarPartitionEvolutionAccessor accessor;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() {
        // Assuming there's a method to reset the connection or mock it anew if necessary
    }

    @Test
    public void testInsertSuccess() throws SQLException {
        // Arrange
        List<ColumnarPartitionEvolutionRecord> records = new ArrayList<>();
        ColumnarPartitionEvolutionRecord record = new ColumnarPartitionEvolutionRecord();
        record.partitionId = 1L;
        record.tableId = 2L;
        record.partitionName = "partitionName";
        record.status = 1;
        record.versionId = 3L;
        record.ddlJobId = 4L;
        record.partitionRecord = new TablePartitionRecord();
        records.add(record);

        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setLong, new Long[] {record.partitionId}));
        params.put(2, new ParameterContext(ParameterMethod.setLong, new Long[] {record.tableId}));
        params.put(3, new ParameterContext(ParameterMethod.setString, new String[] {record.partitionName}));
        params.put(4, new ParameterContext(ParameterMethod.setInt, new Integer[] {record.status}));
        params.put(5, new ParameterContext(ParameterMethod.setLong, new Long[] {record.versionId}));
        params.put(6, new ParameterContext(ParameterMethod.setLong, new Long[] {record.ddlJobId}));
        params.put(7,
            new ParameterContext(ParameterMethod.setString, new String[] {record.partitionRecord.toString()}));

        when(connection.isClosed()).thenReturn(false);
        doNothing().when(connection).commit();

        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        when(connection.prepareStatement(anyString())).thenReturn(ps);

        // Act
        accessor.insert(records);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testInsertFailure() throws SQLException {
        // Arrange
        List<ColumnarPartitionEvolutionRecord> records = new ArrayList<>();
        ColumnarPartitionEvolutionRecord record = new ColumnarPartitionEvolutionRecord();
        record.partitionId = 1L;
        record.tableId = 2L;
        record.partitionName = "partitionName";
        record.status = 1;
        record.versionId = 3L;
        record.ddlJobId = 4L;
        record.partitionRecord = new TablePartitionRecord();
        records.add(record);

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(anyString(),
                anyList(), any())).thenThrow(SQLException.class);

            // Act
            accessor.insert(records);
        }
    }

    @Test
    public void testSelect() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<ColumnarPartitionEvolutionRecord> recordList = new ArrayList<>();
            recordList.add(new ColumnarPartitionEvolutionRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(ColumnarPartitionEvolutionRecord.class), any())).thenReturn(recordList);
            ColumnarPartitionEvolutionAccessor columnarPartitionEvolutionAccessor =
                new ColumnarPartitionEvolutionAccessor();

            List<ColumnarPartitionEvolutionRecord> result =
                columnarPartitionEvolutionAccessor.queryIds(new ArrayList<Long>() {{
                    add(1L);
                }});
            Assert.assertEquals(1, result.size());

            result =
                columnarPartitionEvolutionAccessor.queryTableIdVersionIdOrderById(12L, 1L);
            Assert.assertEquals(1, result.size());

            result =
                columnarPartitionEvolutionAccessor.queryTableIdAndNotInStatus(12L, 1L, 0);
            Assert.assertEquals(1, result.size());

            result =
                columnarPartitionEvolutionAccessor.queryIdsWithOrder(new ArrayList<Long>() {{
                    add(0L);
                }});
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void testUpdatePartitionIdAsIdSuccess() {
        long tableId = 1L;
        long versionId = 2L;

        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, versionId);

        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any())).thenReturn(1);
            accessor.updatePartitionIdAsId(tableId, versionId);
        }
    }

    @Test
    public void testDeletePartitionIdAsIdSuccess() {
        long tableId = 1L;
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableId);

        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), anyMap(), any())).thenReturn(1);
            accessor.deleteId(tableId);
        }
    }
}

