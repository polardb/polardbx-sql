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

public class WeightGroupDataSourceHolder implements GroupDataSourceHolder {

    private final TAtomDataSource masterDataSource;
    private final List<TAtomDataSource> slaveDataSources;
    private OptimizedWeightRandom readWeightRandomWithMaster;
    private OptimizedWeightRandom readWeightRandomSlaveOnly;

    public WeightGroupDataSourceHolder(TAtomDataSource masterDataSource, List<TAtomDataSource> slaveDataSources,
                                       OptimizedWeightRandom readWeightRandomWithMaster,
                                       OptimizedWeightRandom readWeightRandomSlaveOnly) {
        this.masterDataSource = masterDataSource;
        this.slaveDataSources = slaveDataSources;
        this.readWeightRandomWithMaster = readWeightRandomWithMaster;
        this.readWeightRandomSlaveOnly = readWeightRandomSlaveOnly;
    }

    @Override
    public TAtomDataSource getDataSource(MasterSlave masterSlave) {
        switch (masterSlave) {
        case MASTER_ONLY:
            return masterDataSource;
        case READ_WEIGHT:
            if (GeneralUtil.isEmpty(slaveDataSources)) {
                return masterDataSource;
            }
            return (TAtomDataSource) readWeightRandomWithMaster.next();
        case SLAVE_FIRST:
            if (GeneralUtil.isEmpty(slaveDataSources)) {
                return masterDataSource;
            }
            if (slaveDataSources.size() == 1) {
                return slaveDataSources.get(0);
            }
            return (TAtomDataSource) readWeightRandomSlaveOnly.next();
        case SLAVE_ONLY:
            if (slaveDataSources.size() == 1) {
                return slaveDataSources.get(0);
            }
            return (TAtomDataSource) readWeightRandomSlaveOnly.next();
        }
        return masterDataSource;
    }

}
