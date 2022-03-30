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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.topology.ServerInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author moyi
 * @since 2021/11
 */

@TaskName(name = "UpdateNodeStatusTask")
@Getter
@Setter
public class UpdateNodeStatusTask extends BaseDdlTask {

    private static Logger LOG = SQLRecorderLogger.ddlLogger;

    private List<String> dnInstIdList;
    private List<String> cnIpPortList;
    private int beforeStatus;
    private int nodeStatus;
    private boolean ignoreRollback;

    @JSONCreator
    public UpdateNodeStatusTask(String schema,
                                  List<String> dataNodeList,
                                  List<String> cnIpPortList,
                                  int beforeStatus,
                                  int nodeStatus,
                                boolean ignoreRollback) {
        super(schema);
        this.dnInstIdList = dataNodeList;
        this.cnIpPortList = cnIpPortList;
        this.beforeStatus = beforeStatus;
        this.nodeStatus = nodeStatus;
        this.ignoreRollback = ignoreRollback;
    }

    private void applyImpl(Connection metaDbConn, ExecutionContext ec, int status) {
        final String instId = InstIdUtil.getInstId();
        try {
            metaDbConn.setAutoCommit(false);

            boolean notify = false;
            if (CollectionUtils.isNotEmpty(this.dnInstIdList)) {
                StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
                storageInfoAccessor.setConnection(metaDbConn);

                for (String storageInstId : this.dnInstIdList) {
                    // update storage status
                    storageInfoAccessor.updateStorageStatus(storageInstId, status);

                    notify = true;
                }

                // update op-version
                MetaDbConfigManager.getInstance()
                    .notify(MetaDbDataIdBuilder.getStorageInfoDataId(instId), metaDbConn);
            }

            if (CollectionUtils.isNotEmpty(this.cnIpPortList)) {
                ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessor();
                serverInfoAccessor.setConnection(metaDbConn);

                for (String ipPort : this.cnIpPortList) {
                    Pair<String, Integer> ipAndPort = AddressUtils.getIpPortPairByAddrStr(ipPort);

                    // TODO(moyi) update op version?
                    // update server status
                    serverInfoAccessor.updateServerStatusByIpPort( ipAndPort.getKey(), ipAndPort.getValue(), status);
                }
            }

            metaDbConn.commit();
            metaDbConn.setAutoCommit(true);

            if (notify) {
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getStorageInfoDataId(instId));
            }

            LOG.info(getDescription());

        } catch (SQLException e) {
            LOG.error("Failed to " + this.getDescription());
            throw GeneralUtil.nestedException(e);
        }

    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        applyImpl(metaDbConnection, executionContext, this.nodeStatus);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDb, ExecutionContext ec) {
        if (!ignoreRollback) {
            applyImpl(metaDb, ec, this.beforeStatus);
        }
    }

    @Override
    public String getDescription() {
        return String.format("Update node dn(%s) to status %s", StringUtils.join(dnInstIdList), nodeStatus);
    }

}
