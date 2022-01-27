package com.alibaba.polardbx.group.config;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.node.StorageStatus;
import com.alibaba.polardbx.gms.node.StorageStatusManager;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataSourceHolderTest {

    @Test
    public void testMasterSlaveGroupDataSourceHolder() {
        TAtomDataSource master = Mockito.mock(TAtomDataSource.class);
        List<TAtomDataSource> sourceList = new ArrayList<>();
        sourceList.add(Mockito.mock(TAtomDataSource.class));
        sourceList.add(Mockito.mock(TAtomDataSource.class));
        sourceList.add(Mockito.mock(TAtomDataSource.class));
        List<String> sourceIdList = new ArrayList<>();
        sourceIdList.add("1");
        sourceIdList.add("2");
        sourceIdList.add("3");
        MasterSlaveGroupDataSourceHolder slaveGroupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, sourceList, sourceIdList);

        StorageStatusManager.getInstance().allowedReadLearnerIds(sourceIdList.stream().collect(Collectors.toSet()));

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
        TAtomDataSource master = Mockito.mock(TAtomDataSource.class);
        List<TAtomDataSource> sourceList = new ArrayList<>();
        TAtomDataSource ob1 = Mockito.mock(TAtomDataSource.class);
        TAtomDataSource ob2 = Mockito.mock(TAtomDataSource.class);
        TAtomDataSource ob3 = Mockito.mock(TAtomDataSource.class);
        sourceList.add(ob1);
        sourceList.add(ob2);
        sourceList.add(ob3);
        List<String> sourceIdList = new ArrayList<>();
        sourceIdList.add("1");
        sourceIdList.add("2");
        sourceIdList.add("3");
        MasterSlaveGroupDataSourceHolder slaveGroupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, sourceList, sourceIdList);

        StorageStatusManager.getInstance().allowedReadLearnerIds(sourceIdList.stream().collect(Collectors.toSet()));

        Map<String, StorageStatus> map = new HashMap<>();
        map.put("1", new StorageStatus("1", 1, 1, true, true));
        map.put("2", new StorageStatus("1", 1, 1, true, true));
        map.put("3", new StorageStatus("1", 1, 1, false, false));
        StorageStatusManager.getInstance().setStorageStatus(map);
        for (int i = 0; i < 100; i++) {
            Assert.assertTrue(slaveGroupDataSourceHolder.getDataSource(MasterSlave.SLAVE_ONLY) == ob3);
        }
    }

    @Test
    public void testMasterSlaveFirstGroupDataSourceHolder() {
        TAtomDataSource master = Mockito.mock(TAtomDataSource.class);
        List<TAtomDataSource> sourceList = new ArrayList<>();
        TAtomDataSource ob1 = Mockito.mock(TAtomDataSource.class);
        TAtomDataSource ob2 = Mockito.mock(TAtomDataSource.class);
        TAtomDataSource ob3 = Mockito.mock(TAtomDataSource.class);
        sourceList.add(ob1);
        sourceList.add(ob2);
        sourceList.add(ob3);
        List<String> sourceIdList = new ArrayList<>();
        sourceIdList.add("1");
        sourceIdList.add("2");
        sourceIdList.add("3");
        MasterSlaveGroupDataSourceHolder slaveGroupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, sourceList, sourceIdList);

        StorageStatusManager.getInstance().allowedReadLearnerIds(sourceIdList.stream().collect(Collectors.toSet()));

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
        TAtomDataSource master = Mockito.mock(TAtomDataSource.class);
        List<TAtomDataSource> sourceList = new ArrayList<>();
        TAtomDataSource ob1 = Mockito.mock(TAtomDataSource.class);
        TAtomDataSource ob2 = Mockito.mock(TAtomDataSource.class);
        TAtomDataSource ob3 = Mockito.mock(TAtomDataSource.class);
        sourceList.add(ob1);
        sourceList.add(ob2);
        sourceList.add(ob3);
        List<String> sourceIdList = new ArrayList<>();
        sourceIdList.add("1");
        sourceIdList.add("2");
        sourceIdList.add("3");
        MasterSlaveGroupDataSourceHolder slaveGroupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, sourceList, sourceIdList);

        StorageStatusManager.getInstance().allowedReadLearnerIds(sourceIdList.stream().collect(Collectors.toSet()));

        Map<String, StorageStatus> map = new HashMap<>();
        map.put("1", new StorageStatus("1", 1, 1, true, false));
        map.put("2", new StorageStatus("1", 1, 1, true, false));
        map.put("3", new StorageStatus("1", 1, 1, true, false));
        StorageStatusManager.getInstance().setStorageStatus(map);
        Assert.assertTrue(slaveGroupDataSourceHolder.getDataSource(MasterSlave.LOW_DELAY_SLAVE_ONLY) != master);
    }

    @Test(expected = RuntimeException.class)
    public void testMasterLowSlaveGroupDataSourceHolder1() {
        TAtomDataSource master = Mockito.mock(TAtomDataSource.class);
        List<TAtomDataSource> sourceList = new ArrayList<>();
        TAtomDataSource ob1 = Mockito.mock(TAtomDataSource.class);
        TAtomDataSource ob2 = Mockito.mock(TAtomDataSource.class);
        TAtomDataSource ob3 = Mockito.mock(TAtomDataSource.class);
        sourceList.add(ob1);
        sourceList.add(ob2);
        sourceList.add(ob3);
        List<String> sourceIdList = new ArrayList<>();
        sourceIdList.add("1");
        sourceIdList.add("2");
        sourceIdList.add("3");
        MasterSlaveGroupDataSourceHolder slaveGroupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, sourceList, sourceIdList);

        StorageStatusManager.getInstance().allowedReadLearnerIds(sourceIdList.stream().collect(Collectors.toSet()));

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
        List<String> sourceIdList = new ArrayList<>();
        sourceIdList.add("1");
        MasterSlaveGroupDataSourceHolder slaveGroupDataSourceHolder =
            new MasterSlaveGroupDataSourceHolder(master, sourceList, sourceIdList);

        StorageStatusManager.getInstance().allowedReadLearnerIds(sourceIdList.stream().collect(Collectors.toSet()));

        Map<String, StorageStatus> map = new HashMap<>();
        map.put("1", new StorageStatus("1", 1, 1, true, false));
        StorageStatusManager.getInstance().setStorageStatus(map);
        Assert.assertTrue(slaveGroupDataSourceHolder.getDataSource(MasterSlave.LOW_DELAY_SLAVE_ONLY) != master);
    }
}
