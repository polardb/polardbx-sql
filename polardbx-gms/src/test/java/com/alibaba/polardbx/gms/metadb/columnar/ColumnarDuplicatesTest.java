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

package com.alibaba.polardbx.gms.metadb.columnar;

import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.table.ColumnarDuplicatesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarDuplicatesRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ColumnarDuplicatesTest {

    @Test
    public void testSystemTable() {
        Assert.assertTrue(GmsSystemTables.contains("columnar_duplicates"));
    }

    @Test
    public void testFileAccessor() {
        final FilesAccessor ignore = new FilesAccessor();
    }

    @Test
    public void testMetaLogUtil() {
        Assert.assertTrue(DdlMetaLogUtil.isDdlTable("columnar_duplicates"));
    }

    @Mock
    ResultSet rs;

    @Test
    public void testFillRecord0() throws SQLException {
        when(rs.getLong(matches("id"))).thenReturn(1L);
        when(rs.getString(matches("engine"))).thenReturn("test");
        when(rs.getString(matches("logical_schema"))).thenReturn("test");
        when(rs.getString(matches("logical_table"))).thenReturn("test");
        when(rs.getString(matches("partition_name"))).thenReturn("test");
        when(rs.getLong(matches("long_pk"))).thenReturn(1L);
        when(rs.getBytes(matches("bytes_pk"))).thenReturn(new byte[0]);
        when(rs.getString(matches("type"))).thenReturn("double_insert");
        when(rs.getLong(matches("before_file_id"))).thenReturn(1L);
        when(rs.getLong(matches("before_pos"))).thenReturn(1L);
        when(rs.getLong(matches("after_file_id"))).thenReturn(1L);
        when(rs.getLong(matches("after_pos"))).thenReturn(1L);
        when(rs.getString(matches("extra"))).thenReturn("test");
        when(rs.getTimestamp(matches("gmt_created"))).thenReturn(null);
        when(rs.getTimestamp(matches("gmt_modified"))).thenReturn(null);

        final ColumnarDuplicatesRecord result = new ColumnarDuplicatesRecord().fill(rs);
        Truth.assertThat(result.id).isEqualTo(1L);
        Truth.assertThat(result.engine).isEqualTo("test");
        Truth.assertThat(result.logicalSchema).isEqualTo("test");
        Truth.assertThat(result.logicalTable).isEqualTo("test");
        Truth.assertThat(result.partitionName).isEqualTo("test");
        Truth.assertThat(result.longPk).isEqualTo(1L);
        Truth.assertThat(result.bytesPk).isEqualTo(new byte[0]);
        Truth.assertThat(result.type).isEqualTo(ColumnarDuplicatesRecord.Type.DOUBLE_INSERT.getName());
        Truth.assertThat(result.beforeFileId).isEqualTo(1L);
        Truth.assertThat(result.beforePos).isEqualTo(1L);
        Truth.assertThat(result.afterFileId).isEqualTo(1L);
        Truth.assertThat(result.afterPos).isEqualTo(1L);
        Truth.assertThat(result.extra).isEqualTo("test");

        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.executeBatch(anyString(), anyList(), any()))
                .thenReturn(new int[] {1});

            final ColumnarDuplicatesAccessor accessor = new ColumnarDuplicatesAccessor();
            accessor.insert(ImmutableList.of(result));
        }
    }

    @Test
    public void testFillRecord1() throws SQLException {
        when(rs.getLong(matches("id"))).thenReturn(1L);
        when(rs.getString(matches("engine"))).thenReturn("test");
        when(rs.getString(matches("logical_schema"))).thenReturn("test");
        when(rs.getString(matches("logical_table"))).thenReturn("test");
        when(rs.getString(matches("partition_name"))).thenReturn(null);
        when(rs.getLong(matches("long_pk"))).thenReturn(0L);
        when(rs.wasNull()).thenReturn(true);
        when(rs.getBytes(matches("bytes_pk"))).thenReturn(null);
        when(rs.getString(matches("type"))).thenReturn("double_delete");
        when(rs.getLong(matches("before_file_id"))).thenReturn(0L);
        when(rs.getLong(matches("before_pos"))).thenReturn(0L);
        when(rs.getLong(matches("after_file_id"))).thenReturn(0L);
        when(rs.getLong(matches("after_pos"))).thenReturn(0L);
        when(rs.getString(matches("extra"))).thenReturn(null);
        when(rs.getTimestamp(matches("gmt_created"))).thenReturn(null);
        when(rs.getTimestamp(matches("gmt_modified"))).thenReturn(null);

        final ColumnarDuplicatesRecord result = new ColumnarDuplicatesRecord().fill(rs);
        Truth.assertThat(result.id).isEqualTo(1L);
        Truth.assertThat(result.engine).isEqualTo("test");
        Truth.assertThat(result.logicalSchema).isEqualTo("test");
        Truth.assertThat(result.logicalTable).isEqualTo("test");
        Truth.assertThat(result.partitionName).isEqualTo(null);
        Truth.assertThat(result.longPk).isEqualTo(null);
        Truth.assertThat(result.bytesPk).isEqualTo(null);
        Truth.assertThat(result.type).isEqualTo(ColumnarDuplicatesRecord.Type.DOUBLE_DELETE.getName());
        Truth.assertThat(result.beforeFileId).isEqualTo(null);
        Truth.assertThat(result.beforePos).isEqualTo(null);
        Truth.assertThat(result.afterFileId).isEqualTo(null);
        Truth.assertThat(result.afterPos).isEqualTo(null);
        Truth.assertThat(result.extra).isEqualTo(null);

        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.executeBatch(anyString(), anyList(), any()))
                .thenReturn(new int[] {1});

            final ColumnarDuplicatesAccessor accessor = new ColumnarDuplicatesAccessor();
            accessor.insert(ImmutableList.of(result));

            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.executeBatch(anyString(), anyList(), any()))
                .thenReturn(new int[] {0});

            try {
                accessor.insert(ImmutableList.of(result));
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertEquals("ColumnarDuplicates partly inserts.", e.getMessage());
            }

            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.executeBatch(anyString(), anyList(), any()))
                .thenThrow(new RuntimeException("throw exception"));

            try {
                accessor.insert(ImmutableList.of(result));
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertEquals("throw exception", e.getMessage());
            }
        }
    }
}
