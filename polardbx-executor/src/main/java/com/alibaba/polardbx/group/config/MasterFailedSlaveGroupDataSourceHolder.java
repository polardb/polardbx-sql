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
            return masterDataSource;
        case SLAVE_ONLY:
        case LOW_DELAY_SLAVE_ONLY:
            throw new RuntimeException("all slave is failed, so can't continue use slave connection!");
        }
        return masterDataSource;
    }
}
