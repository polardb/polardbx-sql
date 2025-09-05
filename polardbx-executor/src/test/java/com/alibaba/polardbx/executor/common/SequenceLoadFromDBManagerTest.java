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

package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.sequence.Sequence;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.NEW_SEQ;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.TIME_BASED;

public class SequenceLoadFromDBManagerTest {
    @Test
    public void testGetGroupSequence() throws Exception {
        ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
        try {
            final SequenceLoadFromDBManager mgr = new SequenceLoadFromDBManager("test", Collections.emptyMap());

            try (final MockedStatic<MetaDbUtil> metaDbUtil = Mockito.mockStatic(MetaDbUtil.class)) {
                final Connection conn = Mockito.mock(Connection.class);
                metaDbUtil.when(MetaDbUtil::getConnection).thenReturn(conn);
                final PreparedStatement ps = Mockito.mock(PreparedStatement.class);
                Mockito.when(conn.prepareStatement(Mockito.anyString())).thenReturn(ps);
                final ResultSet rs = Mockito.mock(ResultSet.class);
                Mockito.when(ps.executeQuery()).thenReturn(rs);

                // good
                Mockito.when(rs.next()).thenReturn(true);
                Sequence seq = mgr.getGroupSequence("test");
                Assert.assertNotNull(seq);

                // null
                Mockito.when(rs.next()).thenReturn(false);
                seq = mgr.getGroupSequence("test");
                Assert.assertNull(seq);

                // throw 1146
                Mockito.when(rs.next()).thenThrow(new SQLException("", "42S02", 1146))
                    .thenThrow(new SQLException("", "", 1317));
                seq = mgr.getGroupSequence("test");
                Assert.assertNull(seq);

                // throw 1317
                try {
                    mgr.getGroupSequence("test");
                    Assert.fail();
                } catch (SequenceException e) {
                    Assert.assertTrue(e.getCause() instanceof SQLException);
                }
            }
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }

    @Test
    public void testGetVariousSequences() throws Exception {
        ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
        try {
            final SequenceLoadFromDBManager mgr = new SequenceLoadFromDBManager("test", Collections.emptyMap());

            try (final MockedStatic<MetaDbUtil> metaDbUtil = Mockito.mockStatic(MetaDbUtil.class)) {
                final Connection conn = Mockito.mock(Connection.class);
                metaDbUtil.when(MetaDbUtil::getConnection).thenReturn(conn);
                final PreparedStatement ps = Mockito.mock(PreparedStatement.class);
                Mockito.when(conn.prepareStatement(Mockito.anyString())).thenReturn(ps);
                final ResultSet rs = Mockito.mock(ResultSet.class);
                Mockito.when(ps.executeQuery()).thenReturn(rs);

                // good
                Mockito.when(rs.getInt(1)).thenReturn(0);
                Mockito.when(rs.next()).thenReturn(true);
                Sequence seq = mgr.getVariousSequences("test");
                Assert.assertNotNull(seq);

                Mockito.when(rs.getInt(1)).thenReturn(NEW_SEQ);
                Mockito.when(rs.next()).thenReturn(true);
                seq = mgr.getVariousSequences("test");
                Assert.assertNull(seq);

                Mockito.when(rs.getInt(1)).thenReturn(TIME_BASED);
                Mockito.when(rs.next()).thenReturn(true);
                seq = mgr.getVariousSequences("test");
                Assert.assertNotNull(seq);

                // null
                Mockito.when(rs.next()).thenReturn(false);
                seq = mgr.getVariousSequences("test");
                Assert.assertNull(seq);

                // throw 1146
                Mockito.when(rs.next()).thenThrow(new SQLException("", "42S02", 1146))
                    .thenThrow(new SQLException("", "", 1317));
                seq = mgr.getVariousSequences("test");
                Assert.assertNull(seq);

                // throw 1317
                try {
                    mgr.getVariousSequences("test");
                    Assert.fail();
                } catch (SequenceException e) {
                    Assert.assertTrue(e.getCause() instanceof SQLException);
                }
            }
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }
}
