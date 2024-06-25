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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gsi.GsiManager;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ColumnarCheckTableTest {

    private final String schemaName = "schema_name";
    private final String tableName = "table_name";
    private final String indexName = "index_name";

    private GsiMetaManager.GsiMetaBean gsiMetaBean;

    @Mock
    private Map<String, GsiMetaManager.GsiTableMetaBean> gsiTableMeta;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        gsiMetaBean = new GsiMetaManager.GsiMetaBean();
        gsiMetaBean.setTableMeta(gsiTableMeta);
    }

    Map<String, GsiMetaManager.GsiIndexMetaBean> indexMap = new HashMap<>();
    GsiMetaManager.GsiIndexMetaBean indexMetaBean =
        new GsiMetaManager.GsiIndexMetaBean(null, schemaName, tableName, true, schemaName, indexName,
            Collections.emptyList(), null, null, null, null, null, null,
            IndexStatus.PUBLIC, 1, true, true, IndexVisibility.VISIBLE);

    GsiMetaManager.GsiIndexMetaBean indexMetaBean1 =
        new GsiMetaManager.GsiIndexMetaBean(null, schemaName, tableName, true, schemaName, indexName,
            Collections.emptyList(), null, null, null, null, null, null,
            IndexStatus.PUBLIC, 1, true, false, IndexVisibility.VISIBLE);

    GsiMetaManager.GsiTableMetaBean tableMetaBean = new GsiMetaManager.GsiTableMetaBean(null,
        schemaName, tableName, GsiMetaManager.TableType.SHARDING, null, null, null,
        null, null, null, indexMap, null, null);

    @Test
    public void CheckTableWithCciTest() throws Exception {
        LogicalCheckTableHandler logicalCheckTableHandler = mock(LogicalCheckTableHandler.class);
        ExecutionContext executionContext = spy(new ExecutionContext());
        ExecutorContext ec = mock(ExecutorContext.class);
        GsiManager gsiManager = mock(GsiManager.class);
        GsiMetaManager gsiMetaManager = mock(GsiMetaManager.class);
        ArrayResultCursor resultCursor = spy(new ArrayResultCursor(tableName));

        when(ec.getGsiManager()).thenReturn(gsiManager);
        when(gsiManager.getGsiMetaManager()).thenReturn(gsiMetaManager);
        when(gsiMetaManager.getTableAndIndexMeta(any(), any())).thenReturn(gsiMetaBean);

        try (MockedStatic<ExecutorContext> mockedEc = mockStatic(ExecutorContext.class)) {
            mockedEc.when(() -> ExecutorContext.getContext(any())).thenReturn(ec);

            Method privateMethod = logicalCheckTableHandler.getClass()
                .getDeclaredMethod("doCheckForOnePartTableGsi", String.class, String.class, ExecutionContext.class,
                    ArrayResultCursor.class);
            privateMethod.setAccessible(true);

            indexMap.clear();
            indexMap.put("myIndex", indexMetaBean);
            privateMethod.invoke(logicalCheckTableHandler, schemaName, tableName, executionContext, resultCursor);
        }
    }
}
