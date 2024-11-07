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

package com.alibaba.polardbx;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.repo.mysql.handler.LogicalInsertHandler;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class GlobalIndexMetaTest {
    @Test
    public void isExecuteIndexTest() {
        try (MockedStatic<GlobalIndexMeta> globalIndexMeta = mockStatic(GlobalIndexMeta.class)) {
            LogicalInsertHandler handler = new LogicalInsertHandler(null);
            LogicalInsert logicalInsert = mock(LogicalInsert.class);
            ExecutionContext executionContext = mock(ExecutionContext.class);
            Parameters parameters = mock(Parameters.class);
            when(logicalInsert.hasHint()).thenReturn(false);
            when(executionContext.getParams()).thenReturn(parameters);
            globalIndexMeta.when(() -> GlobalIndexMeta.hasGsi("schemaName", "tableName", executionContext))
                .thenReturn(true);
            handler.isExecuteIndex(logicalInsert, executionContext, "schemaName", "tableName");
        }
    }

}
