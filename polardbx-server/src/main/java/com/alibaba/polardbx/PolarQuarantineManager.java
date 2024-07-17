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

package com.alibaba.polardbx;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.QuarantineConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.privilege.quarantine.QuarantineConfigAccessor;
import com.alibaba.polardbx.gms.util.InstIdUtil;

import java.sql.Connection;
import java.sql.Statement;

/**
 * @author shicai.xsc 2020/3/13 15:06
 * @since 5.0.0.0
 */
public class PolarQuarantineManager {

    private static final Logger logger = LoggerFactory.getLogger(PolarQuarantineManager.class);

    private static MetaDbDataSource metaDbDataSource;

    private static PolarQuarantineManager instance;

    private QuarantineConfig quarantine;

    protected static class PrivilegeQuarantineConfigListener implements ConfigListener {

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            logger.info(String.format("start reload quarantine config, newOpVersion: %d", newOpVersion));
            PolarQuarantineManager.getInstance().reload();
            logger.info("finish reload quarantine config");
        }
    }

    public static PolarQuarantineManager getInstance() {
        if (instance == null) {
            synchronized (PolarQuarantineManager.class) {
                if (instance == null) {
                    instance = new PolarQuarantineManager();
                    instance.init();
                }
            }
        }
        return instance;
    }

    private PolarQuarantineManager() {
        quarantine = new QuarantineConfig();
    }

    public boolean checkQuarantine(String host) {
        if (quarantine.getClusterBlacklist() != null && quarantine.getClusterBlacklist().containsOf(host)) {
            return false;
        }

        if (isTrustedIp(host)) {
            return true;
        }

        if (quarantine.getHosts() == null || !quarantine.getHosts().containsKey(InstIdUtil.getInstId())) {
            return false;
        }

        return quarantine.getHosts().get(InstIdUtil.getInstId()).containsOf(host);
    }

    public boolean isTrustedIp(String host) {
        return quarantine.getTrustedIps().containsOf(host);
    }

    public void init() {
        if (metaDbDataSource == null) {
            metaDbDataSource = MetaDbDataSource.getInstance();
            reload();
            registerConfigListener();
        }
    }

    private synchronized void reload() {
        QuarantineConfig tmp = new QuarantineConfig();
        Connection connection = null;
        Statement statement = null;
        try {
            try {
                logger.info("Start reload quarantine data");
                connection = metaDbDataSource.getConnection();
                statement = connection.createStatement();

                QuarantineConfigAccessor quarantineConfigAccessor = new QuarantineConfigAccessor();
                quarantineConfigAccessor.setConnection(connection);
                String config = quarantineConfigAccessor.getAllQuarntineConfigStr(InstIdUtil.getInstId());
                tmp.resetHosts(InstIdUtil.getInstId(), config);

                // add trusted ips
                String trustedIps = "";
                tmp.resetTrustedIps(trustedIps);

                quarantine = tmp;
                logger.info("End reload quarantine data");
            } finally {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        } catch (Throwable e) {
            logger.error("Failed to reload quarantine config: " + e.getMessage());
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "query", GmsSystemTables.QUARANTINE_CONFIG, e.getMessage());
        }
    }

    private void registerConfigListener() {
        String configId = String
            .format(MetaDbDataIdBuilder.getQuarantineConfigDataId(InstIdUtil.getInstId()), InstIdUtil.getInstId());
        MetaDbConfigManager.getInstance().register(configId, null);
        MetaDbConfigManager.getInstance().bindListener(configId, new PrivilegeQuarantineConfigListener());
    }
}
