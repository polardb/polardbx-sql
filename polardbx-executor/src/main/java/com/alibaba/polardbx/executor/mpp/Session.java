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

package com.alibaba.polardbx.executor.mpp;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.mpp.execution.SessionRepresentation;
import com.alibaba.polardbx.executor.mpp.execution.StageId;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.IMppTsoTransaction;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.AUTO_COMMIT_SINGLE_SHARD;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.TSO_TRANSACTION;
import static com.alibaba.polardbx.util.MoreObjects.toStringHelper;

public final class Session {
    private final String queryId;
    private StageId stageId;
    private final long startTime;
    private ExecutionContext clientContext;
    private final boolean preferLocal;
    private boolean localResultIsSync = false;
    private HashMap<String, String> groups = new HashMap<>();
    private long tsoTime = -1;
    private boolean omitTso;
    private boolean lizard1PC;
    private HashMap<String, Long> lsns = new HashMap<>();
    private boolean cacheOutput;
    private boolean ignoreSplitInfo = false;

    public Session(String queryId, ExecutionContext clientContext) {
        this.queryId = queryId;
        this.clientContext = clientContext;
        this.startTime = clientContext.getStartTime();
        this.preferLocal = clientContext.getParamManager().getBoolean(ConnectionParams.MPP_RPC_LOCAL_ENABLED);
    }

    public Session(StageId stageId, ExecutionContext clientContext) {
        this(stageId.getQueryId(), clientContext);
        this.stageId = stageId;
    }

    public HashMap<String, String> getGroups() {
        return groups;
    }

    public boolean isLocalResultIsSync() {
        return localResultIsSync;
    }

    public void setLocalResultIsSync(boolean localResultIsSync) {
        this.localResultIsSync = localResultIsSync;
    }

    public String getQueryId() {
        return queryId;
    }

    public StageId getStageId() {
        return stageId;
    }

    public String getCatalog() {
        return clientContext.getAppName();
    }

    public String getSchema() {
        return clientContext.getSchemaName();
    }

    public String getUser() {
        return clientContext.getConnection() != null ? clientContext.getConnection().getUser() : "";
    }

    public String getEncoding() {
        return clientContext.getEncoding();
    }

    public Map<String, Object> getServerVariables() {
        return clientContext.getServerVariables();
    }

    public Map<String, Object> getUserDefVariables() {
        return clientContext.getUserDefVariables();
    }

    public long getStartTime() {
        return startTime;
    }

    public boolean isPreferLocal() {
        return preferLocal;
    }

    public void generateTsoInfo() throws SQLException {
        if (ExecutorContext.getContext(
            getSchema()).getStorageInfoManager().supportTso() &&
            clientContext.getParamManager().getBoolean(ConnectionParams.ENABLE_CONSISTENT_REPLICA_READ) &&
            !getSchema().equalsIgnoreCase("MetaDB")) {
            ITransaction iTransaction = clientContext.getTransaction();
            if (iTransaction.getTransactionClass().isA(TSO_TRANSACTION)) {
                if (iTransaction.getTransactionClass() == AUTO_COMMIT_SINGLE_SHARD) {
                    final StorageInfoManager storageInfoManager =
                        ExecutorContext.getContext(getSchema()).getStorageInfoManager();
                    this.lizard1PC = storageInfoManager.supportLizard1PCTransaction();
                    this.omitTso = storageInfoManager.supportCtsTransaction() || this.lizard1PC;
                }
                if (!omitTso) {
                    this.tsoTime = ((IMppTsoTransaction) clientContext.getTransaction()).nextTimestamp();
                }

                for (Map.Entry<String, String> group : groups.entrySet()) {
                    ExecUtils.getLsn(group, lsns);
                }
            }
        }
    }

    public SessionRepresentation toSessionRepresentation() {
        Map<String, Object> hintCmds = null;
        if (clientContext.getHintCmds() != null) {
            hintCmds = new HashMap<>(clientContext.getHintCmds());
            // Properties can not be serialized to json
            hintCmds.remove(ConnectionProperties.JOIN_HINT);
        }
        String host = null;
        if (clientContext.getPrivilegeContext() != null) {
            host = clientContext.getPrivilegeContext().getHost();
        }

        if (StringUtils.isEmpty(host)) {
            host = clientContext.getClientIp();
        }

        if (hintCmds == null) {
            hintCmds = new HashMap<>();
        }

        if (!hintCmds.containsKey(ConnectionProperties.ENABLE_CONSISTENT_REPLICA_READ)) {
            hintCmds.put(
                ConnectionProperties.ENABLE_CONSISTENT_REPLICA_READ,
                clientContext.getParamManager().getBoolean(ConnectionParams.ENABLE_CONSISTENT_REPLICA_READ));
        }

        if (!hintCmds.containsKey(ConnectionProperties.DELAY_EXECUTION_STRATEGY)) {
            if (clientContext.getParamManager().getBoolean(ConnectionParams.KEEP_DELAY_EXECUTION_STRATEGY)) {
                hintCmds.put(
                    ConnectionProperties.DELAY_EXECUTION_STRATEGY,
                    clientContext.getParamManager().getInt(ConnectionParams.DELAY_EXECUTION_STRATEGY));
            }

            if (!hintCmds.containsKey(ConnectionProperties.DELAY_EXECUTION_STRATEGY)) {
                if (clientContext.getParamManager().getBoolean(ConnectionParams.KEEP_DELAY_EXECUTION_STRATEGY)) {
                    hintCmds.put(
                        ConnectionProperties.DELAY_EXECUTION_STRATEGY,
                        clientContext.getParamManager().getInt(ConnectionParams.DELAY_EXECUTION_STRATEGY));
                }
            }
        }

        if (!hintCmds.containsKey(ConnectionProperties.MPP_METRIC_LEVEL)) {
            //mpp默认是sql级别的采样
            hintCmds.put(
                ConnectionProperties.MPP_METRIC_LEVEL, MetricLevel.SQL.metricLevel);
        }

        return new SessionRepresentation(
            clientContext.getTraceId(),
            getCatalog(),
            getSchema(),
            getUser(),
            host,
            getEncoding(),
            clientContext.getMdcConnString(),
            clientContext.getSqlMode(),
            clientContext.getTxIsolation(),
            clientContext.getSocketTimeout(),
            clientContext.isEnableTrace(),
            startTime,
            getServerVariables(),
            getUserDefVariables(),
            hintCmds,
            clientContext.getParams(),
            clientContext.getCacheRelNodeIds(),
            clientContext.getRecordRowCnt(),
            clientContext.isTestMode(),
            clientContext.getConnection().getLastInsertId(),
            clientContext.getTimeZone(),
            tsoTime,
            lsns,
            omitTso,
            lizard1PC,
            clientContext.getWorkloadType());
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("queryId", queryId)
            .add("catalog", getCatalog())
            .add("schema", getSchema())
            .add("user", getUser())
            .add("encoding", getEncoding())
            .add("params", clientContext.getParams())
            .omitNullValues()
            .toString();
    }

    public ExecutionContext getClientContext() {
        return clientContext;
    }

    public boolean isCacheOutput() {
        return cacheOutput;
    }

    public void setCacheOutput(boolean cacheOutput) {
        this.cacheOutput = cacheOutput;
    }

    public boolean isIgnoreSplitInfo() {
        return ignoreSplitInfo;
    }

    public void setIgnoreSplitInfo(boolean ignoreSplitInfo) {
        this.ignoreSplitInfo = ignoreSplitInfo;
    }
}