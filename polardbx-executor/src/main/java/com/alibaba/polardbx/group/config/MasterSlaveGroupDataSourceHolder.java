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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.node.StorageStatus;
import com.alibaba.polardbx.gms.node.StorageStatusManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class MasterSlaveGroupDataSourceHolder implements GroupDataSourceHolder {

    private final TAtomDataSource masterDataSource;
    private final List<TAtomDataSource> slaveDataSources;
    private final List<String> slaveStorageIds;
    private Random random;

    public MasterSlaveGroupDataSourceHolder(
        TAtomDataSource masterDataSource, List<TAtomDataSource> slaveDataSources, List<String> slaveStorageIds) {
        this.masterDataSource = masterDataSource;
        this.slaveDataSources = slaveDataSources;
        this.slaveStorageIds = slaveStorageIds;
        this.random = new Random(System.currentTimeMillis());
    }

    @Override
    public TAtomDataSource getDataSource(MasterSlave masterSlave) {
        switch (masterSlave) {
        case MASTER_ONLY:
        case READ_WEIGHT:
            return masterDataSource;
        case SLAVE_FIRST:
            if (GeneralUtil.isEmpty(slaveDataSources)) {
                return masterDataSource;
            }
            return selectLowDelaySlaveDataSource(true);
        case SLAVE_ONLY:
            //FIXME 只有一个备库的时候，则直接选择(即便不允许备库读的DN也会被强制路由)
            if (slaveDataSources.size() == 1) {
                return slaveDataSources.get(0);
            }
            return selectSlaveDataSource();
        case LOW_DELAY_SLAVE_ONLY:
            return selectLowDelaySlaveDataSource(false);
        }
        return masterDataSource;
    }

    private TAtomDataSource selectSlaveDataSource() {
        Map<String, StorageStatus> statusMap = StorageStatusManager.getInstance().getAllowReadLearnerStorageMap();
        int startIndex = random.nextInt(slaveDataSources.size());
        //基于延迟和负载均衡策略，选择符合要求的备库路由
        for (int i = 0; i < slaveStorageIds.size(); i++) {
            String id = slaveStorageIds.get(startIndex);
            StorageStatus storageStatus = statusMap.get(id);
            if (storageStatus != null && (!storageStatus.isBusy() && !storageStatus.isDelay())) {
                return slaveDataSources.get(startIndex);
            }
            startIndex++;
            if (startIndex >= slaveStorageIds.size()) {
                startIndex = 0;
            }
        }

        //若不满足要求，则选择允许备库读的DN路由
        for (int i = 0; i < slaveStorageIds.size(); i++) {
            String id = slaveStorageIds.get(startIndex);
            StorageStatus storageStatus = statusMap.get(id);
            if (storageStatus != null) {
                return slaveDataSources.get(startIndex);
            }
            startIndex++;
            if (startIndex >= slaveStorageIds.size()) {
                startIndex = 0;
            }
        }

        //FIXME 若都不满足要求，则选择第一个路由(即便不允许备库读的DN也会被强制路由)
        return slaveDataSources.get(startIndex);
    }

    private TAtomDataSource selectLowDelaySlaveDataSource(boolean forceMaster) {
        Map<String, StorageStatus> statusMap = StorageStatusManager.getInstance().getAllowReadLearnerStorageMap();

        int startIndex = random.nextInt(slaveDataSources.size());
        List<Pair<String, Integer>> lowDelayIds = new ArrayList<>();
        //挑选出低延迟备库
        for (int i = 0; i < slaveStorageIds.size(); i++) {
            String id = slaveStorageIds.get(startIndex);
            StorageStatus storageStatus = statusMap.get(id);
            if (storageStatus != null && !storageStatus.isDelay()) {
                lowDelayIds.add(new Pair<>(id, startIndex));
            }
            startIndex++;
            if (startIndex >= slaveStorageIds.size()) {
                startIndex = 0;
            }
        }
        if (lowDelayIds.isEmpty()) {
            if (forceMaster) {
                //没有低延迟备库时候，直接路由主库
                return masterDataSource;
            } else {
                throw new RuntimeException("all slave is delay, so can't continue use slave connection!");
            }

        } else {
            //从低延迟备库集合中，选择负载低的备库做路由
            for (Pair<String, Integer> pair : lowDelayIds) {
                StorageStatus storageStatus = statusMap.get(pair.getKey());
                if (storageStatus != null && !storageStatus.isBusy()) {
                    return slaveDataSources.get(pair.getValue());
                }
            }
            //如果负载低的备库，则选择第一个路由
            return slaveDataSources.get(lowDelayIds.get(0).getValue());
        }
    }
}
