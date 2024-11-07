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

import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mockStatic;

@RunWith(MockitoJUnitRunner.class)
public class ColumnarDropDbTest {

    @InjectMocks
    private TableInfoManager tableInfoManager;

    @Test
    public void testUpdateColumnarTableStatusAndVersionIDBySchema() {
        // Arrange
        String schema = "test_schema";
        long versionId = 123L;
        String status = "DROP";

        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger updateCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> updateCount.get());

            // Act
            tableInfoManager.updateColumnarTableStatusAndVersionIDBySchema(schema, versionId, status);
        }
    }
}

