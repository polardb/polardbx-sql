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

package com.alibaba.polardbx.executor.partitionmanagement;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CheckCciPartitionTest {
    @Mock
    private ExecutionContext executionContext;

    @Mock
    private TableMeta tableMeta;

    @Mock
    private ParamManager paramManager;

    private static final String TABLE_NAME = "test_table";

    @Before
    public void setUp() {
        when(executionContext.getParamManager()).thenReturn(paramManager);

    }

    @Test
    public void testDropNoException() {
        when(paramManager.getBoolean(any())).thenReturn(true);
        AlterTableGroupUtils.checkAllDropTruncateCciPartition(executionContext, tableMeta, TABLE_NAME);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testDropException1() {
        when(paramManager.getBoolean(any())).thenReturn(false);
        when(tableMeta.withCci()).thenReturn(true);
        AlterTableGroupUtils.checkAllDropTruncateCciPartition(executionContext, tableMeta, TABLE_NAME);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testDropException2() {
        when(paramManager.getBoolean(any())).thenReturn(false);
        when(tableMeta.withCci()).thenReturn(false);
        when(tableMeta.isColumnar()).thenReturn(true);
        AlterTableGroupUtils.checkAllDropTruncateCciPartition(executionContext, tableMeta, TABLE_NAME);
    }

    @Test
    public void testModifyNoException() {
        when(paramManager.getBoolean(any())).thenReturn(true);
        AlterTableGroupUtils.checkAllModifyListCciPartition(executionContext, tableMeta, TABLE_NAME);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testModifyException1() {
        when(paramManager.getBoolean(any())).thenReturn(false);
        when(tableMeta.withCci()).thenReturn(true);
        AlterTableGroupUtils.checkAllModifyListCciPartition(executionContext, tableMeta, TABLE_NAME);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testModifyException2() {
        when(paramManager.getBoolean(any())).thenReturn(false);
        when(tableMeta.withCci()).thenReturn(false);
        when(tableMeta.isColumnar()).thenReturn(true);
        AlterTableGroupUtils.checkAllModifyListCciPartition(executionContext, tableMeta, TABLE_NAME);
    }
}
