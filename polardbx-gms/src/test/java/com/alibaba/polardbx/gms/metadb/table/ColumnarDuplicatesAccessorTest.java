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

package com.alibaba.polardbx.gms.metadb.table;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class ColumnarDuplicatesAccessorTest {
    @Test
    public void testCount() throws Exception {
        final ColumnarDuplicatesAccessor columnarDuplicatesAccessor = new ColumnarDuplicatesAccessor();
        final Connection connection = Mockito.mock(Connection.class);
        columnarDuplicatesAccessor.setConnection(connection);
        final PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(preparedStatement);
        final ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);
        Mockito.when(resultSet.getLong(Mockito.anyInt())).thenReturn(1L);

        long l = columnarDuplicatesAccessor.countDuplicates(1L);
        Assert.assertEquals(1, l);
        l = columnarDuplicatesAccessor.countDuplicates(1L);
        Assert.assertEquals(0, l);

        Mockito.when(resultSet.next()).thenThrow(new RuntimeException("mock"));
        try {
            columnarDuplicatesAccessor.countDuplicates(1L);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("mock"));
        }
    }
}
