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

import java.util.List;
import java.util.Random;

public class MasterSlaveGroupDataSourceHolder implements GroupDataSourceHolder {

    private final TAtomDataSource masterDataSource;
    private final List<TAtomDataSource> slaveDataSources;
    private Random random;

    public MasterSlaveGroupDataSourceHolder(TAtomDataSource masterDataSource, List<TAtomDataSource> slaveDataSources) {
        this.masterDataSource = masterDataSource;
        this.slaveDataSources = slaveDataSources;
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
            if (slaveDataSources.size() == 1) {
                return slaveDataSources.get(0);
            }
            return slaveDataSources.get(random.nextInt(slaveDataSources.size() - 1));
        case SLAVE_ONLY:
            if (slaveDataSources.size() == 1) {
                return slaveDataSources.get(0);
            }
            return slaveDataSources.get(random.nextInt(slaveDataSources.size() - 1));
        }
        return masterDataSource;
    }
}
