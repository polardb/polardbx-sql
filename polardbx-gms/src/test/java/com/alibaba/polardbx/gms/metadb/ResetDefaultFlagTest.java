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

import com.alibaba.polardbx.gms.metadb.table.ColumnsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;

public class ResetDefaultFlagTest {

    String schemaName = "schema_name";
    String tableName = "table_name";
    String columnName = "column_name";

    @Test
    public void resetDefaultExprFlagTest() throws Exception {
        TableInfoManager tableInfoManager = new TableInfoManager();
        ColumnsAccessor columnsAccessor = Mockito.mock(ColumnsAccessor.class);

        Field field1 = TableInfoManager.class.getDeclaredField("columnsAccessor");
        field1.setAccessible(true);
        field1.set(tableInfoManager, columnsAccessor);

        Mockito.when(
                columnsAccessor.resetColumnFlag(schemaName, tableName, columnName, ColumnsRecord.FLAG_DEFAULT_EXPR))
            .thenReturn(1);
        tableInfoManager.resetColumnDefaultExprFlag(schemaName, tableName, columnName);
    }
}
