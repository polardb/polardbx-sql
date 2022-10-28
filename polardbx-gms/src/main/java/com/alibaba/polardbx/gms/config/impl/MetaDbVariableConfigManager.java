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

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.config.VariableConfigManager;
import com.alibaba.polardbx.gms.config.VariableConfigReceiver;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.VariableConfigAccessor;
import com.alibaba.polardbx.gms.topology.VariableConfigRecord;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author youtianyu
 */
public class MetaDbVariableConfigManager extends AbstractLifecycle implements VariableConfigManager {
    protected volatile Map<String, Object> dnVariableConfigMap = new HashMap<>();
    protected static final MetaDbVariableConfigManager instance = new MetaDbVariableConfigManager();

    public static MetaDbVariableConfigManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    protected MetaDbVariableConfigManager() {
//        doInit();
    }

    @Override
    protected void doInit() {
        reloadVariableConfig();
    }

    @Override
    public void reloadVariableConfig() {
        synchronized (this) {
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                reloadVariableConfigMapFromMetaDb(metaDbConn);
            } catch (Throwable t) {
                throw GeneralUtil.nestedException(t);
            }
        }
    }

    private void reloadVariableConfigMapFromMetaDb(Connection metaDbConnection) {
        try {
            Map<String, Object> tempVariableConfig = new HashMap<>();
            VariableConfigAccessor variableConfigAccessor = new VariableConfigAccessor();
            variableConfigAccessor.setConnection(metaDbConnection);
            List<VariableConfigRecord> variableConfigRecordList = variableConfigAccessor.queryAll();
            MetaDbInstConfigManager instConfigManager = MetaDbInstConfigManager.getInstance();
            instConfigManager.reloadInstConfig();
            for (VariableConfigRecord record : variableConfigRecordList) {
                if ("true".equalsIgnoreCase(record.paramValue)) {
                    tempVariableConfig.put(record.paramKey, true);
                } else if ("false".equalsIgnoreCase(record.paramValue)) {
                    tempVariableConfig.put(record.paramKey, false);
                } else {
                    tempVariableConfig.put(record.paramKey, record.paramValue);
                }
            }
            this.dnVariableConfigMap = tempVariableConfig;
        } catch (Throwable t) {
            throw GeneralUtil.nestedException(t);
        }
    }

    public Map<String, Object> getDnVariableConfigMap() {
        return dnVariableConfigMap;
    }

    public static class MetaDbVariableConfigListener implements ConfigListener {
        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            getInstance().reloadVariableConfig();
        }
    }
}