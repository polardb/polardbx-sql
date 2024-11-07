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

package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CciMetaManagerTest {

    @Spy
    CciMetaManager cciMetaManagerSpy;

    List<ColumnarTableEvolutionRecord> evolutionRecords = new ArrayList<>();
    List<TablePartitionRecord> partitionRecords = new ArrayList<>();

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        ColumnarTableEvolutionRecord record = new ColumnarTableEvolutionRecord();
        record.tableId = 1L;
        record.tableName = "test_table";
        record.tableSchema = "test_schema";
        record.indexName = "test_index";
        record.versionId = 100L;
        record.ddlJobId = 101L;
        record.partitions = new java.util.ArrayList<>();
        evolutionRecords.add(record);

        TablePartitionRecord partitionRecord = new TablePartitionRecord();
        partitionRecords.add(partitionRecord);

    }

    @Test
    public void testDoInit() {
        ColumnarTableEvolutionAccessor columnarTableEvolutionAccessor = mock(ColumnarTableEvolutionAccessor.class);
        ColumnarPartitionEvolutionAccessor columnarPartitionEvolutionAccessor =
            mock(ColumnarPartitionEvolutionAccessor.class);
        TablePartitionAccessor tablePartition = mock(TablePartitionAccessor.class);
        when(columnarTableEvolutionAccessor.queryPartitionEmptyRecords()).thenReturn(evolutionRecords);
        when(tablePartition.getTablePartitionsByDbNameTbName(anyString(), anyString(), anyBoolean())).thenReturn(
            partitionRecords);

        when(cciMetaManagerSpy.getColumnarTableEvolution()).thenReturn(columnarTableEvolutionAccessor);
        when(cciMetaManagerSpy.getColumnarPartitionEvolution()).thenReturn(columnarPartitionEvolutionAccessor);
        when(cciMetaManagerSpy.getTablePartition()).thenReturn(tablePartition);

        try (MockedStatic<ConfigDataMode> configDataModeMockedStatic = mockStatic(ConfigDataMode.class)) {
            configDataModeMockedStatic.when(ConfigDataMode::isPolarDbX).thenReturn(true);

            try (MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic = mockStatic(MetaDbDataSource.class)) {
                MetaDbDataSource metaDbDataSourceMock = mock(MetaDbDataSource.class);
                Connection connectionMock = mock(Connection.class);
                when(metaDbDataSourceMock.getConnection()).thenReturn(connectionMock);
                metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(metaDbDataSourceMock);

                cciMetaManagerSpy.init();
                verify(cciMetaManagerSpy, times(1)).doInit();
                verify(cciMetaManagerSpy, times(1)).loadPartitions();
            }

        }
    }

    @Test
    public void testDoInitNotPolarDBX() {
        try (MockedStatic<ConfigDataMode> configDataModeMockedStatic = mockStatic(ConfigDataMode.class)) {
            configDataModeMockedStatic.when(ConfigDataMode::isPolarDbX).thenReturn(false);

            cciMetaManagerSpy.init();
            verify(cciMetaManagerSpy, times(1)).doInit();
            verify(cciMetaManagerSpy, times(0)).loadPartitions(); // 验证是否没有调用loadPartitions
        }
    }
}



