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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;

@RunWith(MockitoJUnitRunner.class)
public class RenameTableTest {

    @Mock
    private Connection metaDbConnection;
    @Mock
    private ExecutionContext executionContext;

    @InjectMocks
    private RenameColumnarTablesMetaTask task1;

    private RenameColumnarTableMetaTask task2;

    private static final String SCHEMA_NAME = "testSchema";
    private static final String OLD_TABLE_NAME = "oldTableName";
    private static final String NEW_TABLE_NAME = "newTableName";
    private static final Long VERSION_ID = 1L;
    private static final Long JOB_ID = 1L;

    @Before
    public void setUp() {
        task1 = new RenameColumnarTablesMetaTask(SCHEMA_NAME,
            java.util.Collections.singletonList(OLD_TABLE_NAME),
            java.util.Collections.singletonList(NEW_TABLE_NAME),
            java.util.Collections.singletonList(VERSION_ID));
        task1.setJobId(JOB_ID);

        task2 = new RenameColumnarTableMetaTask(SCHEMA_NAME,
            OLD_TABLE_NAME,
            NEW_TABLE_NAME,
            VERSION_ID);
        task2.setJobId(JOB_ID);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testRenameTables() throws Exception {
        // When: executeImpl method is called.
        task1.executeImpl(metaDbConnection, executionContext);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testRenameTablesRollback() throws Exception {
        // When: executeImpl method is called.
        task1.rollbackImpl(metaDbConnection, executionContext);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testRenameTable() throws Exception {
        // When: executeImpl method is called.
        task2.executeImpl(metaDbConnection, executionContext);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testRenameTableRollback() throws Exception {
        // When: executeImpl method is called.
        task2.rollbackImpl(metaDbConnection, executionContext);
    }
}

