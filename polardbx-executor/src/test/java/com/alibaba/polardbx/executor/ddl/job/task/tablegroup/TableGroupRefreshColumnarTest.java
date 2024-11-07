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

import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupRefreshMetaBaseTask;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TableGroupRefreshColumnarTest {

    @Mock
    private TablePartitionAccessor tablePartitionAccessor;
    @Mock
    private ColumnarTableMappingAccessor columnarTableMappingAccessor;
    @Mock
    private ColumnarPartitionEvolutionAccessor columnarPartitionEvolutionAccessor;
    @Mock
    private ColumnarTableEvolutionAccessor columnarTableEvolutionAccessor;

    @InjectMocks
    private AlterTableGroupRefreshMetaBaseTask task;

    private final static String TABLE_GROUP_NAME = "tg_test";
    private final static String SCHEMA_NAME = "db_test";
    private final static String TABLE_NAME = "tb_test";
    private final static long VERSION_ID = 1L;
    private final static long JOB_ID = 2L;

    @Before
    public void setUp() {
        task = new AlterTableGroupRefreshMetaBaseTask(SCHEMA_NAME, TABLE_GROUP_NAME, VERSION_ID);
    }

    @Test
    public void testUpdateColumnarEvolutionSysTables() {
        TableMeta tableMeta = mock(TableMeta.class);
        when(tableMeta.isColumnar()).thenReturn(true);

        List<ColumnarPartitionEvolutionRecord> evolutionRecords = new ArrayList<>();
        ColumnarPartitionEvolutionRecord record = new ColumnarPartitionEvolutionRecord();
        record.id = 3L;
        evolutionRecords.add(record);

        when(columnarPartitionEvolutionAccessor.queryIdsWithOrder(anyList())).thenReturn(evolutionRecords);

        List<TablePartitionRecord> partitionRecords = new ArrayList<>();
        TablePartitionRecord partitionRecord1 = new TablePartitionRecord();
        TablePartitionRecord partitionRecord2 = new TablePartitionRecord();
        TablePartitionRecord partitionRecordOld1 = new TablePartitionRecord();
        TablePartitionRecord partitionRecordOld2 = new TablePartitionRecord();
        partitionRecord1.partName = "";
        // rename partition name
        partitionRecord2.partName = "p10";
        partitionRecords.add(partitionRecord1);
        partitionRecords.add(partitionRecord2);
        partitionRecordOld1.partName = "";
        partitionRecordOld2.partName = "p1";

        when(tablePartitionAccessor.getTablePartitionsByDbNameTbName(eq(SCHEMA_NAME), eq(TABLE_NAME), eq(false)))
            .thenReturn(partitionRecords);

        List<ColumnarTableMappingRecord> columnarTableMappingRecords = new ArrayList<>();
        ColumnarTableMappingRecord columnarTableMappingRecord = new ColumnarTableMappingRecord();
        columnarTableMappingRecord.tableId = 1L;
        columnarTableMappingRecords.add(columnarTableMappingRecord);

        ColumnarTableEvolutionRecord columnarTableEvolutionRecord = new ColumnarTableEvolutionRecord();
        columnarTableEvolutionRecord.partitions = new ArrayList<>();
        columnarTableEvolutionRecord.partitions.add(0L);
        columnarTableEvolutionRecord.partitions.add(1L);

        List<ColumnarPartitionEvolutionRecord> columnarPartitionEvolutionRecords = new ArrayList<>();
        ColumnarPartitionEvolutionRecord record1 =
            new ColumnarPartitionEvolutionRecord(1L, "", 1L, 1L, partitionRecordOld1, 1);
        ColumnarPartitionEvolutionRecord record2 =
            new ColumnarPartitionEvolutionRecord(1L, "p1", 1L, 1L, partitionRecordOld2, 1);
        columnarPartitionEvolutionRecords.add(record1);
        columnarPartitionEvolutionRecords.add(record2);

        when(columnarTableMappingAccessor.querySchemaIndex(eq(SCHEMA_NAME), eq(TABLE_NAME))).thenReturn(
            columnarTableMappingRecords);

        when(columnarPartitionEvolutionAccessor.queryIdsWithOrder(anyList())).thenReturn(
            columnarPartitionEvolutionRecords);

        when(columnarTableEvolutionAccessor.queryTableIdLatest(eq(1L))).thenReturn(
            Collections.singletonList(columnarTableEvolutionRecord));

        when(columnarPartitionEvolutionAccessor.queryTableIdAndNotInStatus(anyLong(), anyLong(), anyLong())).thenReturn(
            columnarPartitionEvolutionRecords);

        task.updateColumnarEvolutionSysTables(tableMeta, TABLE_NAME, tablePartitionAccessor,
            columnarTableMappingAccessor, columnarPartitionEvolutionAccessor, columnarTableEvolutionAccessor,
            VERSION_ID, JOB_ID);

        verify(columnarPartitionEvolutionAccessor, times(1)).insert(anyList());
        verify(columnarTableMappingAccessor, times(1)).updateVersionId(eq(VERSION_ID), anyLong());
    }
}

