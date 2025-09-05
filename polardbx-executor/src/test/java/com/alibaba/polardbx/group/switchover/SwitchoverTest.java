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

package com.alibaba.polardbx.group.switchover;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.atom.config.TAtomDsConfDO;
import com.alibaba.polardbx.atom.config.gms.TAtomDsGmsConfigHelper;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfig;
import com.alibaba.polardbx.group.config.MasterFailedSlaveGroupDataSourceHolder;
import com.alibaba.polardbx.group.config.MasterOnlyGroupDataSourceHolder;
import com.alibaba.polardbx.group.config.MasterSlaveGroupDataSourceHolder;
import com.alibaba.polardbx.rpc.client.XClient;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.packet.XPacket;
import com.alibaba.polardbx.rpc.perf.SwitchoverPerfCollection;
import com.alibaba.polardbx.rpc.pool.XClientPool;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.mysql.cj.polarx.protobuf.PolarxNotice;
import com.mysql.cj.x.protobuf.Polarx;
import com.mysql.cj.x.protobuf.PolarxDatatypes;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SwitchoverTest {
    public final static String SERVER_ADDR = "11.167.60.147:14120";
    public final static int SERVER_PORT = ThreadLocalRandom.current().nextInt(65530) + 1;
    public final static String SERVER_USR = "diamond";
    public final static String SERVER_PSW_ENC = "STA0HLmViwmos2woaSzweB9QM13NjZgrOtXTYx+ZzLw=";
    public final static String SERVER_DB = "test";

    private TAtomDataSource atomDS(TAtomDataSource.AtomSourceFrom sourceFrom, String dnId, int xport) {
        ConnPoolConfig storageInstConfig = new ConnPoolConfig();
        storageInstConfig.minPoolSize = 1;
        storageInstConfig.maxPoolSize = 1;
        storageInstConfig.maxWaitThreadCount = 1;
        storageInstConfig.idleTimeout = 60000;
        storageInstConfig.blockTimeout = 5000;
        storageInstConfig.connProps = "";
        storageInstConfig.xprotoStorageDbPort = 0;
        TAtomDsConfDO atomDsConf =
            TAtomDsGmsConfigHelper.buildAtomDsConfByGms(SERVER_ADDR, xport, SERVER_USR, SERVER_PSW_ENC, SERVER_DB,
                storageInstConfig, SERVER_DB);
        atomDsConf.setXport(xport);
        atomDsConf.setCharacterEncoding("utf8mb4");
        TAtomDataSource atomDs = new TAtomDataSource(sourceFrom, dnId);
        atomDs.init("app", "group" + dnId, "dsKey" + dnId, "", atomDsConf);
        return atomDs;
    }

    @Test
    public void testBuildDS() throws SQLException, IOException {
        TAtomDataSource dn1 = atomDS(TAtomDataSource.AtomSourceFrom.MASTER_DB, "dn1", SERVER_PORT);
        final SwitchoverPerfCollection collector =
            dn1.getDataSource().unwrap(XDataSource.class).getSwitchoverPerfCollector();
        collector.recordWait(true, 0);
        collector.recordWait(false, 0);
        collector.record(0);
        collector.record(1);
        collector.reset();
        Assert.assertFalse(collector.toString().isEmpty());

        final BiFunction<XClient, XPacket, Boolean> packetConsumer =
            XConnectionManager.getInstance().getPacketConsumer();
        final PolarxNotice.SessionStateChanged.Builder builder = PolarxNotice.SessionStateChanged.newBuilder();
        builder.setParam(PolarxNotice.SessionStateChanged.Parameter.EXTRA_SERVER_STATE);
        builder.setValue(PolarxDatatypes.Scalar.newBuilder().setType(PolarxDatatypes.Scalar.Type.V_UINT)
            .setVUnsignedInt(PolarxNotice.SessionStateChanged.ExtraServerState.IN_LEADER_TRANSFER_FLAG_VALUE).build());
        final ByteBuffer allocate = ByteBuffer.allocate(2048);
        final CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(allocate);
        builder.build().writeTo(codedOutputStream);
        codedOutputStream.flush();
        final PolarxNotice.Frame.Builder frameBuilder = PolarxNotice.Frame.newBuilder();
        frameBuilder.setPayload(ByteString.copyFrom(allocate.array(), 0, allocate.position()));
        frameBuilder.setType(PolarxNotice.Frame.Type.SESSION_STATE_CHANGED_VALUE);
        final XPacket packet = new XPacket(123, Polarx.ServerMessages.Type.NOTICE_VALUE, frameBuilder.build());
        final XClientPool pool = new XClientPool(XConnectionManager.getInstance(), SERVER_ADDR, SERVER_PORT, SERVER_USR, SERVER_PSW_ENC);
        pool.getInstInfo().add("test");
        final XClient client = mock(XClient.class);
        when(client.getPool()).thenReturn(pool);

        packetConsumer.apply(client, packet);
    }

    @Test
    public void testMasterFailedSlaveGroupDataSourceHolder() {
        final MasterFailedSlaveGroupDataSourceHolder groupDataSourceHolder =
            new MasterFailedSlaveGroupDataSourceHolder(
                atomDS(TAtomDataSource.AtomSourceFrom.MASTER_DB, "dn1", SERVER_PORT));
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.MASTER_ONLY).getKey());
    }

    @Test
    public void testMasterFailedSlaveGroupDataSourceHolderThrow() {
        final TAtomDataSource ds = mock(TAtomDataSource.class);
        when(ds.getDataSource()).thenThrow(new RuntimeException("mock exception"));
        final MasterFailedSlaveGroupDataSourceHolder groupDataSourceHolder =
            new MasterFailedSlaveGroupDataSourceHolder(ds);
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.MASTER_ONLY).getKey());
    }

    @Test
    public void testMasterFailedSlaveGroupDataSourceHolderNotXRPC() {
        final TAtomDataSource ds = mock(TAtomDataSource.class);
        final DataSource dataSource = mock(DataSource.class);
        when(ds.getDataSource()).thenReturn(dataSource);
        final MasterFailedSlaveGroupDataSourceHolder groupDataSourceHolder =
            new MasterFailedSlaveGroupDataSourceHolder(ds);
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.MASTER_ONLY).getKey());
    }

    @Test
    public void testMasterOnlyGroupDataSourceHolder() {
        final MasterOnlyGroupDataSourceHolder groupDataSourceHolder =
            new MasterOnlyGroupDataSourceHolder(
                atomDS(TAtomDataSource.AtomSourceFrom.MASTER_DB, "dn1", SERVER_PORT + 1));
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.MASTER_ONLY).getKey());
    }

    @Test
    public void testMasterOnlyGroupDataSourceHolderThrow() {
        final TAtomDataSource ds = mock(TAtomDataSource.class);
        when(ds.getDataSource()).thenThrow(new RuntimeException("mock exception"));
        final MasterOnlyGroupDataSourceHolder groupDataSourceHolder = new MasterOnlyGroupDataSourceHolder(ds);
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.MASTER_ONLY).getKey());
    }

    @Test
    public void testMasterOnlyGroupDataSourceHolderNotXRPC() {
        final TAtomDataSource ds = mock(TAtomDataSource.class);
        final DataSource dataSource = mock(DataSource.class);
        when(ds.getDataSource()).thenReturn(dataSource);
        final MasterOnlyGroupDataSourceHolder groupDataSourceHolder = new MasterOnlyGroupDataSourceHolder(ds);
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.MASTER_ONLY).getKey());
    }

    @Test
    public void testMasterSlaveGroupDataSourceHolder() throws SQLException {
        final TAtomDataSource master = spy(atomDS(TAtomDataSource.AtomSourceFrom.MASTER_DB, "dn1", SERVER_PORT + 2));
        final TAtomDataSource learner = atomDS(TAtomDataSource.AtomSourceFrom.LEARNER_DB, "dn2", SERVER_PORT + 3);
        final TAtomDataSource follower = atomDS(TAtomDataSource.AtomSourceFrom.FOLLOWER_DB, "dn3", SERVER_PORT + 4);
        final MasterSlaveGroupDataSourceHolder groupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, ImmutableList.of(learner, follower));
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.MASTER_ONLY).getKey());
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.READ_WEIGHT).getKey());
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.FOLLOWER_ONLY).getKey());
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.SLAVE_FIRST).getKey());
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.SLAVE_ONLY).getKey());
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.LOW_DELAY_SLAVE_ONLY).getKey());

        learner.getDataSource().unwrap(XDataSource.class).getClientPool().markChangingLeader();
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.MASTER_ONLY).getKey());
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.READ_WEIGHT).getKey());
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.FOLLOWER_ONLY).getKey());
        Assert.assertTrue(groupDataSourceHolder.isChangingLeader(MasterSlave.SLAVE_FIRST).getKey());
        Assert.assertTrue(groupDataSourceHolder.isChangingLeader(MasterSlave.SLAVE_ONLY).getKey());
        Assert.assertTrue(groupDataSourceHolder.isChangingLeader(MasterSlave.LOW_DELAY_SLAVE_ONLY).getKey());

        follower.getDataSource().unwrap(XDataSource.class).getClientPool().markChangingLeader();
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.MASTER_ONLY).getKey());
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.READ_WEIGHT).getKey());
        Assert.assertTrue(groupDataSourceHolder.isChangingLeader(MasterSlave.FOLLOWER_ONLY).getKey());
        Assert.assertTrue(groupDataSourceHolder.isChangingLeader(MasterSlave.SLAVE_FIRST).getKey());
        Assert.assertTrue(groupDataSourceHolder.isChangingLeader(MasterSlave.SLAVE_ONLY).getKey());
        Assert.assertTrue(groupDataSourceHolder.isChangingLeader(MasterSlave.LOW_DELAY_SLAVE_ONLY).getKey());

        final MasterSlaveGroupDataSourceHolder groupDataSourceHolderMasterOnly =
            new MasterSlaveGroupDataSourceHolder(master, ImmutableList.of());
        Assert.assertFalse(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.MASTER_ONLY).getKey());
        Assert.assertFalse(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.READ_WEIGHT).getKey());
        Assert.assertFalse(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.FOLLOWER_ONLY).getKey());
        Assert.assertFalse(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.SLAVE_FIRST).getKey());
        Assert.assertFalse(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.SLAVE_ONLY).getKey());
        Assert.assertFalse(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.LOW_DELAY_SLAVE_ONLY).getKey());

        master.getDataSource().unwrap(XDataSource.class).getClientPool().markChangingLeader();
        Assert.assertTrue(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.MASTER_ONLY).getKey());
        Assert.assertTrue(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.READ_WEIGHT).getKey());
        Assert.assertTrue(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.FOLLOWER_ONLY).getKey());
        Assert.assertTrue(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.SLAVE_FIRST).getKey());
        Assert.assertTrue(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.SLAVE_ONLY).getKey());
        Assert.assertFalse(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.LOW_DELAY_SLAVE_ONLY).getKey());

        master.getDataSource().unwrap(XDataSource.class).getClientPool().clearChangingLeaderMark();
        follower.getDataSource().unwrap(XDataSource.class).getClientPool().clearChangingLeaderMark();
        learner.getDataSource().unwrap(XDataSource.class).getClientPool().clearChangingLeaderMark();

        final DataSource dataSource = mock(DataSource.class);
        when(master.getDataSource()).thenReturn(dataSource);
        Assert.assertFalse(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.MASTER_ONLY).getKey());
        Assert.assertFalse(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.READ_WEIGHT).getKey());
        Assert.assertFalse(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.FOLLOWER_ONLY).getKey());
        Assert.assertFalse(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.SLAVE_FIRST).getKey());
        Assert.assertFalse(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.SLAVE_ONLY).getKey());
        Assert.assertFalse(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.LOW_DELAY_SLAVE_ONLY).getKey());
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.SLAVE_FIRST).getKey());

        when(master.getDataSource()).thenThrow(new RuntimeException("mock exception"));
        Assert.assertFalse(groupDataSourceHolderMasterOnly.isChangingLeader(MasterSlave.MASTER_ONLY).getKey());
    }

    @Test
    public void testMasterSlaveGroupDataSourceHolderThrow() {
        final TAtomDataSource ds = mock(TAtomDataSource.class);
        when(ds.getDataSource()).thenThrow(new RuntimeException("mock exception"));
        final MasterSlaveGroupDataSourceHolder groupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(ds, ImmutableList.of());
        Assert.assertFalse(groupDataSourceHolder.isChangingLeader(MasterSlave.MASTER_ONLY).getKey());
    }
}
