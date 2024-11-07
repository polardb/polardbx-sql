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

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.util.Collections;

import static com.alibaba.polardbx.common.utils.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class WaitColumnarTableAlterPartitionTaskTest {
    @Mock
    private ExecutionContext executionContext;

    @Mock
    private Connection connection;

    @Mock
    private DdlContext ddlContext;

    @Mock
    MetaDbDataSource metaDbDataSource;

    private MockedStatic<MetaDbDataSource> mockMetaDbDataSource;
    private MockedConstruction<TableInfoManager> mockTableInfoManager;

    private final ColumnarTableMappingRecord mappingRecord = new ColumnarTableMappingRecord();
    private final ColumnarTableEvolutionRecord evolutionRecord = new ColumnarTableEvolutionRecord();
    private final ColumnarCheckpointsRecord checkpointRecord = new ColumnarCheckpointsRecord();

    @Before
    public void setUp() throws Exception {
        mockMetaDbDataSource = Mockito.mockStatic(MetaDbDataSource.class);
        mockMetaDbDataSource.when(MetaDbDataSource::getInstance).thenReturn(metaDbDataSource);
        when(metaDbDataSource.getConnection()).thenReturn(connection);

        mappingRecord.latestVersionId = 1;
        evolutionRecord.commitTs = 1000L;
        checkpointRecord.extra = WaitColumnarTableAlterPartitionTask.ALTER_PARTITION_SUCCESS_CHECKPOINT_TYPE;

        mockTableInfoManager = Mockito.mockConstruction(
            TableInfoManager.class,
            (mock, context) -> {
                Mockito.when(mock.queryColumnarTableMapping(Mockito.anyString(), Mockito.anyString())).thenAnswer(
                    invocation -> Collections.singletonList(mappingRecord)
                );
                Mockito.when(mock.queryColumnarTableEvolutionByVersionId(Mockito.anyLong())).thenAnswer(
                    invocation -> evolutionRecord
                );
                Mockito.when(mock.queryColumnarCheckpointsByCommitTs(Mockito.anyLong())).thenAnswer(
                    invocation -> Collections.singletonList(checkpointRecord)
                );
            });

        when(executionContext.getDdlContext()).thenReturn(ddlContext);
        when(ddlContext.isInterrupted()).thenReturn(false);
    }

    @After
    public void tearDown() throws Exception {
        if (mockMetaDbDataSource != null) {
            mockMetaDbDataSource.close();
        }

        if (mockTableInfoManager != null) {
            mockTableInfoManager.close();
        }
    }

    @Test
    public void testBeforeTransaction_Success() throws Exception {
        String schemaName = "testSchema";
        String indexName = "testIndex";
        WaitColumnarTableAlterPartitionTask task =
            spy(new WaitColumnarTableAlterPartitionTask(schemaName, Collections.singletonList(indexName), false));

        task.beforeTransaction(executionContext);

        // 跳出等待循环
        verify(executionContext, times(1)).getDdlContext();
    }

    @Test
    public void testBeforeTransaction_Failed() throws Exception {
        String schemaName = "testSchema";
        String indexName = "testIndex";
        WaitColumnarTableAlterPartitionTask task =
            spy(new WaitColumnarTableAlterPartitionTask(schemaName, Collections.singletonList(indexName), false));

        checkpointRecord.extra = "";
        when(ddlContext.isInterrupted()).thenReturn(true);

        // 验证抛出异常
        try {
            task.beforeTransaction(executionContext);
            fail("Expected an exception to be thrown"); // 如果未抛出异常，测试失败
        } catch (Exception e) {
            // 验证异常类型和消息
            assertEquals("ERR-CODE: [PXC-4636][ERR_DDL_JOB_ERROR] The job '0' has been interrupted. ", e.getMessage());
        }
    }
}
