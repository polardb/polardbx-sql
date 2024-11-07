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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GlobalIndexMetaTest {
    @Test
    public void hasGsiTest() {
        String mainTableName = "test_table";
        String schemaName = "test_schema";
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        TableMeta tableMeta = Mockito.mock(TableMeta.class);
        Mockito.when(ec.getSchemaManager(schemaName)).thenReturn(schemaManager);
        Mockito.when(schemaManager.getTable(mainTableName)).thenReturn(tableMeta);

        boolean hasGsi;
        Mockito.when(tableMeta.withGsiExcludingPureCci()).thenReturn(true);
        hasGsi = GlobalIndexMeta.hasGsi(mainTableName, schemaName, ec);
        Assert.assertTrue(hasGsi);
    }

    @Test
    public void allCciTest() {
        String tableName = "test_table";
        String indexName1 = "test_index_1";
        String indexName2 = "test_index_2";
        String schemaName = "test_schema";

        Map<String, GsiMetaManager.GsiIndexMetaBean> indexMap1 = new HashMap<>();
        Map<String, GsiMetaManager.GsiIndexMetaBean> indexMap3 = new HashMap<>();
        // cci
        GsiMetaManager.GsiIndexMetaBean indexMetaBean1 =
            new GsiMetaManager.GsiIndexMetaBean(null, schemaName, tableName, true, schemaName, indexName1,
                Collections.emptyList(), null, null, null, null, null, null,
                IndexStatus.PUBLIC, 1, true, true, IndexVisibility.VISIBLE);
        // gsi
        GsiMetaManager.GsiIndexMetaBean indexMetaBean2 =
            new GsiMetaManager.GsiIndexMetaBean(null, schemaName, tableName, true, schemaName, indexName2,
                Collections.emptyList(), null, null, null, null, null, null,
                IndexStatus.PUBLIC, 1, true, false, IndexVisibility.VISIBLE);

        // gsi & cci
        indexMap1.put(indexName1, indexMetaBean1);
        indexMap1.put(indexName2, indexMetaBean2);

        // all cci
        indexMap3.put(indexName1, indexMetaBean1);

        GsiMetaManager.GsiTableMetaBean tableMetaBean1 = new GsiMetaManager.GsiTableMetaBean(null,
            schemaName, tableName, GsiMetaManager.TableType.SHARDING, null, null, null,
            null, null, null, indexMap1, null, null);
        GsiMetaManager.GsiTableMetaBean tableMetaBean3 = new GsiMetaManager.GsiTableMetaBean(null,
            schemaName, tableName, GsiMetaManager.TableType.SHARDING, null, null, null,
            null, null, null, indexMap3, null, null);

        TableMeta tableMeta = Mockito.mock(TableMeta.class);
        Mockito.when(tableMeta.withCci()).thenReturn(true);

        Mockito.when(tableMeta.getGsiTableMetaBean()).thenReturn(tableMetaBean1);
        Mockito.doCallRealMethod().when(tableMeta).allCci();
        boolean allCci = tableMeta.allCci();
        Assert.assertFalse(allCci);

        Mockito.when(tableMeta.getGsiTableMetaBean()).thenReturn(tableMetaBean3);
        Mockito.doCallRealMethod().when(tableMeta).allCci();
        allCci = tableMeta.allCci();
        Assert.assertTrue(allCci);
    }

    @Test
    public void withGsiExcludingPureCciTest() {
        boolean hasGsi;
        TableMeta tableMeta = Mockito.mock(TableMeta.class);
        Mockito.doCallRealMethod().when(tableMeta).withGsiExcludingPureCci();

        Mockito.when(tableMeta.withGsi()).thenReturn(true);
        Mockito.when(tableMeta.allCci()).thenReturn(false);
        hasGsi = tableMeta.withGsiExcludingPureCci();
        Assert.assertTrue(hasGsi);

        Mockito.when(tableMeta.withGsi()).thenReturn(true);
        Mockito.when(tableMeta.allCci()).thenReturn(true);
        hasGsi = tableMeta.withGsiExcludingPureCci();
        Assert.assertFalse(hasGsi);

        Mockito.when(tableMeta.withGsi()).thenReturn(false);
        Mockito.when(tableMeta.allCci()).thenReturn(false);
        tableMeta.withGsiExcludingPureCci();
        Assert.assertFalse(hasGsi);
    }
}
