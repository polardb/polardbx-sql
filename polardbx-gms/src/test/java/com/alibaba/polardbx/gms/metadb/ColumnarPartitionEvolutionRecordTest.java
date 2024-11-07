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

import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionStatus;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ColumnarPartitionEvolutionRecordTest {
    @Mock
    ResultSet rs;

    @Test
    public void testConstructor() {
        final ColumnarPartitionEvolutionRecord record = new ColumnarPartitionEvolutionRecord(
            1L, "p1", 3L, 4L, null, 0);
        assertThat(record.getTableId()).isEqualTo(1L);
        assertThat(record.getPartitionName()).isEqualTo("p1");
        assertThat(record.getVersionId()).isEqualTo(3L);
        assertThat(record.getDdlJobId()).isEqualTo(4L);
    }

    @Test
    public void testFill() throws SQLException {
        when(rs.getLong("id")).thenReturn(1L); // 假定id返回1L
        when(rs.getLong("partition_id")).thenReturn(2L); // 假定partition_id返回2L
        when(rs.getLong("table_id")).thenReturn(3L); // 假定table_id返回3L
        when(rs.getString("partition_name")).thenReturn("partitionName"); // 假定partition_name返回"partitionName"
        when(rs.getInt("status")).thenReturn(0); // 假定status返回0
        when(rs.getLong("version_id")).thenReturn(4L); // 假定version_id返回4L
        when(rs.getLong("ddl_job_id")).thenReturn(5L); // 假定ddl_job_id返回5L
        // 假定partition_record返回的JSON字符串可以被deserializeFromJson方法正确反序列化
        when(rs.getString("partition_record")).thenReturn("{\"key\":\"value\"}");

        final ColumnarPartitionEvolutionRecord result = new ColumnarPartitionEvolutionRecord().fill(rs);
        assertThat(result.getId()).isEqualTo(1L);
        assertThat(result.getPartitionId()).isEqualTo(2L);
        assertThat(result.getTableId()).isEqualTo(3L);
        assertThat(result.getPartitionName()).isEqualTo("partitionName");
        assertThat(result.getStatus()).isEqualTo(0);
        assertThat(result.getVersionId()).isEqualTo(4L);
        assertThat(result.getDdlJobId()).isEqualTo(5L);
    }

    @Test
    public void testConvertAbsent() {
        // 测试边界情况：0 应该转换为 ABSENT
        Assert.assertEquals(ColumnarPartitionStatus.ABSENT, ColumnarPartitionStatus.convert(0));
    }

    @Test
    public void testConvertPublic() {
        // 测试边界情况：1 应该转换为 PUBLIC
        Assert.assertEquals(ColumnarPartitionStatus.PUBLIC, ColumnarPartitionStatus.convert(1));
    }

    @Test
    public void testConvertDefault() {
        // 测试默认情况：除了定义的值以外，都应该转换为 null
        Assert.assertNull(ColumnarPartitionStatus.convert(-1));  // 测试负数
        Assert.assertNull(ColumnarPartitionStatus.convert(2));  // 测试未定义的正数
        // 可以添加更多边界条件和情况
    }
}
