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

/**
 * The slaves is down, then throw exception for SLAVE_ONLY&LOW_DELAY_SLAVE_ONLY
 */
public class MasterFailedSlaveGroupDataSourceHolder implements GroupDataSourceHolder {

    private final TAtomDataSource masterDataSource;

    public MasterFailedSlaveGroupDataSourceHolder(TAtomDataSource masterDataSource) {
        this.masterDataSource = masterDataSource;
    }

    @Override
    public TAtomDataSource getDataSource(MasterSlave masterSlave) {
        switch (masterSlave) {
        case MASTER_ONLY:
        case READ_WEIGHT:
        case SLAVE_FIRST:
        case FOLLOWER_ONLY:
            return masterDataSource;
        case SLAVE_ONLY:
        case LOW_DELAY_SLAVE_ONLY:
            throw new RuntimeException("all slave is failed, so can't continue use slave connection!");
        }
        return masterDataSource;
    }
}
