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

package com.alibaba.polardbx.gms.config.impl;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.VariableConfigAccessor;
import com.alibaba.polardbx.gms.topology.VariableConfigRecord;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;

/**
 * @author youtianyu
 * <p>
 * Usage: call SystemGlobalPropertyHelper.initDefaultVariableConfig()
 * if you need preset some variables on DN,
 * write their values in prepareDnVariable()
 * </p>
 */
public class SystemGlobalPropertyHelper {
    private final static Logger
        logger = LoggerFactory.getLogger(SystemGlobalPropertyHelper.class);

    public static void initDefaultVariableConfig() {
        List<VariableConfigRecord> defaultGlobalVariableConfig = new LinkedList<>();
        prepareDnVariable(defaultGlobalVariableConfig);
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            connection.setAutoCommit(false);
            VariableConfigAccessor variableConfigAccessor = new VariableConfigAccessor();
            variableConfigAccessor.setConnection(connection);
            variableConfigAccessor.addVariableConfigs(defaultGlobalVariableConfig);
            connection.commit();
        } catch (Throwable t) {
            logger
                .warn("Failed to register to system global properties into metadb, err is" + t.getMessage(), t);
            MetaDbLogUtil.META_DB_LOG
                .warn("Failed to register to system global properties into metadb, err is" + t.getMessage(), t);
        }
    }

    private static VariableConfigRecord generateDnConfigRecord(String key, String value) {
        VariableConfigRecord result = new VariableConfigRecord();
        result.paramKey = key;
        result.paramValue = value;
        return result;
    }

    private static void prepareDnVariable(List<VariableConfigRecord> variableConfigRecordList) {
        variableConfigRecordList.add(generateDnConfigRecord("innodb_spin_wait_delay", "6"));
        variableConfigRecordList.add(generateDnConfigRecord("innodb_sync_spin_loops", "30"));
    }
}
