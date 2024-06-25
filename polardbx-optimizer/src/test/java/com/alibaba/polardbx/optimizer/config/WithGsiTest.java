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

package com.alibaba.polardbx.optimizer.config;

import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class WithGsiTest {

    String schemaName = "schema_name";
    String tableName = "table_name";
    String indexName = "index_name";

    @Spy
    TableMeta tableMeta = new TableMeta(schemaName, tableName, Lists.newArrayList(), null, null,
        true, TableStatus.PUBLIC, 1, 1);

    @Test
    public void WiGsiTest() {
        Map<String, GsiMetaManager.GsiIndexMetaBean> indexMap = new HashMap<>();
        GsiMetaManager.GsiIndexMetaBean indexMetaBean1 =
            new GsiMetaManager.GsiIndexMetaBean(null, schemaName, tableName, true, schemaName, indexName,
                Collections.emptyList(), null, null, null, null, null, null,
                IndexStatus.PUBLIC, 1, true, false, IndexVisibility.VISIBLE);

        indexMap.put("myIndex", indexMetaBean1);

        GsiMetaManager.GsiTableMetaBean tableMetaBean = new GsiMetaManager.GsiTableMetaBean(
            "catalog", schemaName, tableName, GsiMetaManager.TableType.SHARDING,
            "partitionKey", "partitionPolicy", 1,
            "tbPartitionKey", "tbPartitionPolicy", 1,
            indexMap, "comment", indexMetaBean1
        );

        when(tableMeta.getGsiTableMetaBean()).thenReturn(tableMetaBean);

        Assert.assertTrue(tableMeta.withGsi());
        Assert.assertFalse(tableMeta.withCci());

        Assert.assertTrue(tableMeta.withGsi("myIndex"));
        Assert.assertTrue(tableMeta.withGsi("MYINDEX"));
        Assert.assertTrue(tableMeta.withGsi("myindex"));
        Assert.assertFalse(tableMeta.withCci("myIndex"));

        GsiMetaManager.GsiIndexMetaBean indexMetaBean2 =
            new GsiMetaManager.GsiIndexMetaBean(null, schemaName, tableName, true, schemaName, indexName,
                Collections.emptyList(), null, null, null, null, null, null,
                IndexStatus.PUBLIC, 1, true, true, IndexVisibility.VISIBLE);
        indexMap.put("myIndex", indexMetaBean2);

        Assert.assertTrue(tableMeta.withGsi());
        Assert.assertTrue(tableMeta.withCci());

        Assert.assertTrue(tableMeta.withGsi("myIndex"));
        Assert.assertTrue(tableMeta.withCci("myIndex"));

        indexMap.clear();
        indexMap.put("myIndex1", indexMetaBean1);
        indexMap.put("myIndex2", indexMetaBean2);

        Assert.assertTrue(tableMeta.withGsi());
        Assert.assertTrue(tableMeta.withCci());

        Assert.assertTrue(tableMeta.withGsi("myIndex1"));
        Assert.assertTrue(tableMeta.withCci("myIndex2"));
    }
}
