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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;

@RunWith(MockitoJUnitRunner.class)
public class CleanSchemaMetaTest {

    @Mock
    private Connection metaDbConn;

    @InjectMocks
    private SchemaMetaUtil.PolarDbXSchemaMetaCleaner polarDbXSchemaMetaCleaner =
        new SchemaMetaUtil.PolarDbXSchemaMetaCleaner();

    @Before
    public void setUp() {
        // Setup procedures if necessary
    }

    @After
    public void tearDown() {
        // Cleanup resources or reset mocks
//        mockReset(metaDbConn);
    }

    @Test
    public void testClearSchemaMetaInvokesCleanupSchemaMeta() throws Exception {
        // Given
        final String schemaName = "test_schema";
        final long versionId = 123L;

        MetaDbConfigManager metaDbConfigManager = Mockito.mock(MetaDbConfigManager.class);
        doNothing().when(metaDbConfigManager).unregister(anyString(), any());
        try (
            MockedStatic<MetaDbDataIdBuilder> mockMetaDbDataIdBuilder = Mockito.mockStatic(MetaDbDataIdBuilder.class)) {
            try (MockedStatic<MetaDbConfigManager> mockMetaDbConfigManager = Mockito.mockStatic(
                MetaDbConfigManager.class)) {
                mockMetaDbDataIdBuilder.when(() -> MetaDbDataIdBuilder.getTableListDataId(anyString()))
                    .thenReturn("table");
                mockMetaDbConfigManager.when(() -> MetaDbConfigManager.getInstance())
                    .thenAnswer(invocation -> metaDbConfigManager);

                polarDbXSchemaMetaCleaner.clearSchemaMeta(schemaName, metaDbConn, versionId);
            }
        }
    }

}
