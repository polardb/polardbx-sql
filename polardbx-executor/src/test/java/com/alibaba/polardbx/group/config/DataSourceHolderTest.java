/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.group.config;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.node.StorageStatus;
import com.alibaba.polardbx.gms.node.StorageStatusManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.when;

public class DataSourceHolderTest {

    List<TAtomDataSource> sourceList = new ArrayList<>();
    Set<String> allowedReadLearnerIds = new HashSet<>();
    TAtomDataSource master;

    @Before
    public void before() {
        allowedReadLearnerIds.add("1");
        allowedReadLearnerIds.add("2");
        allowedReadLearnerIds.add("3");
        StorageStatusManager.getInstance().allowedReadLearnerIds(allowedReadLearnerIds);
        master = Mockito.mock(TAtomDataSource.class);

        TAtomDataSource ob1 = Mockito.mock(TAtomDataSource.class);
        when(ob1.getDnId()).thenReturn("1");
        when(ob1.isFollowerDB()).thenReturn(false);
        TAtomDataSource ob2 = Mockito.mock(TAtomDataSource.class);
        when(ob2.getDnId()).thenReturn("2");
        when(ob2.isFollowerDB()).thenReturn(false);
        TAtomDataSource ob3 = Mockito.mock(TAtomDataSource.class);
        when(ob3.getDnId()).thenReturn("3");
        when(ob3.isFollowerDB()).thenReturn(false);
        sourceList.add(ob1);
        sourceList.add(ob2);
        sourceList.add(ob3);
    }

    @After
    public void after() {
        StorageStatusManager.getInstance().setStorageStatus(new HashMap<>());
        StorageStatusManager.getInstance().allowedReadLearnerIds(new HashSet<>());
    }

    @Test
    public void testMasterSlaveGroupDataSourceHolder() {

        MasterSlaveGroupDataSourceHolder slaveGroupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, sourceList);

        Map<String, StorageStatus> map = new HashMap<>();
        map.put("1", new StorageStatus("1", 1, 1, true, true));
        map.put("2", new StorageStatus("1", 1, 1, true, true));
        map.put("3", new StorageStatus("1", 1, 1, true, true));
        StorageStatusManager.getInstance().setStorageStatus(map);
        slaveGroupDataSourceHolder.getDataSource(MasterSlave.SLAVE_ONLY);
        map.put("1", new StorageStatus("1", 1, 1, false, true));
        map.put("2", new StorageStatus("1", 1, 1, false, true));
        map.put("3", new StorageStatus("1", 1, 1, false, true));
        StorageStatusManager.getInstance().setStorageStatus(map);
        slaveGroupDataSourceHolder.getDataSource(MasterSlave.SLAVE_ONLY);

        map.put("1", new StorageStatus("1", 1, 1, true, false));
        map.put("2", new StorageStatus("1", 1, 1, true, false));
        map.put("3", new StorageStatus("1", 1, 1, true, false));
        StorageStatusManager.getInstance().setStorageStatus(map);
        slaveGroupDataSourceHolder.getDataSource(MasterSlave.SLAVE_ONLY);
    }

    @Test
    public void testMasterSlaveGroupDataSourceHolder1() {
        MasterSlaveGroupDataSourceHolder slaveGroupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, sourceList);
        Map<String, StorageStatus> map = new HashMap<>();
        map.put("1", new StorageStatus("1", 1, 1, true, true));
        map.put("2", new StorageStatus("1", 1, 1, true, true));
        map.put("3", new StorageStatus("1", 1, 1, false, false));
        StorageStatusManager.getInstance().setStorageStatus(map);
        for (int i = 0; i < 100; i++) {
            Assert.assertTrue(slaveGroupDataSourceHolder.getDataSource(MasterSlave.SLAVE_ONLY) == sourceList.get(2));
        }
    }

    @Test
    public void testMasterSlaveFirstGroupDataSourceHolder() {
        MasterSlaveGroupDataSourceHolder slaveGroupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, sourceList);

        Map<String, StorageStatus> map = new HashMap<>();
        map.put("1", new StorageStatus("1", 1, 1, true, true));
        map.put("2", new StorageStatus("1", 1, 1, true, true));
        map.put("3", new StorageStatus("1", 1, 1, true, true));
        StorageStatusManager.getInstance().setStorageStatus(map);
        Assert.assertTrue(slaveGroupDataSourceHolder.getDataSource(MasterSlave.SLAVE_FIRST) == master);
        map.put("1", new StorageStatus("1", 1, 1, false, true));
        map.put("2", new StorageStatus("1", 1, 1, false, true));
        map.put("3", new StorageStatus("1", 1, 1, false, true));
        StorageStatusManager.getInstance().setStorageStatus(map);
        Assert.assertTrue(slaveGroupDataSourceHolder.getDataSource(MasterSlave.SLAVE_FIRST) == master);

        map.put("1", new StorageStatus("1", 1, 1, true, false));
        map.put("2", new StorageStatus("1", 1, 1, true, false));
        map.put("3", new StorageStatus("1", 1, 1, true, false));
        StorageStatusManager.getInstance().setStorageStatus(map);
        Assert.assertTrue(slaveGroupDataSourceHolder.getDataSource(MasterSlave.SLAVE_FIRST) != master);
    }

    @Test
    public void testMasterLowSlaveGroupDataSourceHolder() {
        MasterSlaveGroupDataSourceHolder slaveGroupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, sourceList);

        Map<String, StorageStatus> map = new HashMap<>();
        map.put("1", new StorageStatus("1", 1, 1, true, false));
        map.put("2", new StorageStatus("1", 1, 1, true, false));
        map.put("3", new StorageStatus("1", 1, 1, true, false));
        StorageStatusManager.getInstance().setStorageStatus(map);
        Assert.assertTrue(slaveGroupDataSourceHolder.getDataSource(MasterSlave.LOW_DELAY_SLAVE_ONLY) != master);
    }

    @Test(expected = RuntimeException.class)
    public void testMasterLowSlaveGroupDataSourceHolder1() {
        MasterSlaveGroupDataSourceHolder slaveGroupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, sourceList);

        Map<String, StorageStatus> map = new HashMap<>();
        map.put("1", new StorageStatus("1", 1, 1, true, true));
        map.put("2", new StorageStatus("1", 1, 1, true, true));
        map.put("3", new StorageStatus("1", 1, 1, true, true));
        StorageStatusManager.getInstance().setStorageStatus(map);
        Assert.assertTrue(slaveGroupDataSourceHolder.getDataSource(MasterSlave.LOW_DELAY_SLAVE_ONLY) != master);
    }

    @Test
    public void testMasterLowSlaveGroupDataSourceHolder2() {
        TAtomDataSource master = Mockito.mock(TAtomDataSource.class);
        List<TAtomDataSource> sourceList = new ArrayList<>();
        TAtomDataSource ob1 = Mockito.mock(TAtomDataSource.class);
        sourceList.add(ob1);
        when(ob1.getDnId()).thenReturn("1");
        when(ob1.isFollowerDB()).thenReturn(false);
        MasterSlaveGroupDataSourceHolder slaveGroupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, sourceList);

        Map<String, StorageStatus> map = new HashMap<>();
        map.put("1", new StorageStatus("1", 1, 1, true, false));
        StorageStatusManager.getInstance().setStorageStatus(map);
        Assert.assertTrue(slaveGroupDataSourceHolder.getDataSource(MasterSlave.LOW_DELAY_SLAVE_ONLY) != master);
    }

    @Test
    public void testMasterFollowerDataSourceHolder3() {
        TAtomDataSource master = Mockito.mock(TAtomDataSource.class);
        List<TAtomDataSource> sourceList = new ArrayList<>();
        TAtomDataSource ob1 = Mockito.mock(TAtomDataSource.class);
        sourceList.add(ob1);
        when(ob1.getDnId()).thenReturn("1");
        when(ob1.isFollowerDB()).thenReturn(true);
        TAtomDataSource ob2 = Mockito.mock(TAtomDataSource.class);
        sourceList.add(ob2);
        when(ob2.getDnId()).thenReturn("1");
        when(ob2.isFollowerDB()).thenReturn(true);

        MasterSlaveGroupDataSourceHolder slaveGroupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, sourceList);

        TAtomDataSource ret = slaveGroupDataSourceHolder.getDataSource(MasterSlave.MASTER_ONLY);
        Assert.assertTrue(ret == master);

        ret = slaveGroupDataSourceHolder.getDataSource(MasterSlave.FOLLOWER_ONLY);
        Assert.assertTrue((ret == ob1 || ret == ob2));

        ret = slaveGroupDataSourceHolder.getDataSource(MasterSlave.SLAVE_FIRST);
        Assert.assertTrue((ret == master));

        ret = slaveGroupDataSourceHolder.getDataSource(MasterSlave.SLAVE_ONLY);
        Assert.assertTrue((ret == ob1 || ret == ob2));

        Map<String, StorageStatus> map = new HashMap<>();
        map.put("1", new StorageStatus("1", 1, 1, true, false));
        StorageStatusManager.getInstance().setStorageStatus(map);
        Assert.assertTrue(slaveGroupDataSourceHolder.getDataSource(MasterSlave.LOW_DELAY_SLAVE_ONLY) != master);
    }

    @Test
    public void testMasterFollowerLearnerDataSourceHolder4() {
        TAtomDataSource ob4 = Mockito.mock(TAtomDataSource.class);
        sourceList.add(ob4);
        when(ob4.getDnId()).thenReturn("4");
        when(ob4.isFollowerDB()).thenReturn(true);

        MasterSlaveGroupDataSourceHolder slaveGroupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, sourceList);

        TAtomDataSource ret = slaveGroupDataSourceHolder.getDataSource(MasterSlave.MASTER_ONLY);
        Assert.assertTrue(ret == master);

        ret = slaveGroupDataSourceHolder.getDataSource(MasterSlave.FOLLOWER_ONLY);
        Assert.assertTrue((ret == ob4));

        ret = slaveGroupDataSourceHolder.getDataSource(MasterSlave.SLAVE_FIRST);
        Assert.assertTrue((ret == master));

        ret = slaveGroupDataSourceHolder.getDataSource(MasterSlave.SLAVE_ONLY);
        Assert.assertTrue((ret != ob4 && ret != master));
    }
}
