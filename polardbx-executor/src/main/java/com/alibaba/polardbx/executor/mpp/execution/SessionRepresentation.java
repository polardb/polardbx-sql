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

package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.server.TaskResource;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.memory.QueryMemoryPool;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.alibaba.polardbx.optimizer.statis.SQLTracer;
import com.alibaba.polardbx.optimizer.utils.IColumnarTransaction;
import com.alibaba.polardbx.optimizer.utils.IMppReadOnlyTransaction;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.alibaba.polardbx.util.MoreObjects;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

// Deliver the session Variables based this class from Server to Worker.
public class SessionRepresentation {
    private long queryStart;
    private String traceId;
    private String catalog;
    private String schema;
    private String user;
    private String host;
    private String encoding;
    private String sqlMode;
    private String mdcConnString;
    private int txIsolation;
    private int socketTimeout;
    private boolean enableTrace;
    private Map<String, Object> hintCmds;
    private Map<String, Object> serverVariables;
    private Map<String, Object> userDefVariables;
    private Map<Integer, ParameterContext> params;
    private Set<Integer> cacheRelNodesId;
    private Map<Integer, Integer> recordRowCnt;
    private Map<Integer, Integer> distinctKeyCnt = new HashMap<>();
    private boolean testMode;
    private long lastInsertId;
    private InternalTimeZone logicalTimeZone;
    private long tsoTimeStamp;
    private boolean useColumnar;
    private Map<String, Long> dnLsnMap = new HashMap<>();
    private WorkloadType workloadType;
    private boolean omitTso;
    private boolean lizard1PC;
    private boolean useColumnarTracer;

    /**
     * 暂时只增加polardbx_server_id参数，避免长度增加较多；后续如有需要可以再修改
     */
    private Map<String, Object> extraServerVariables;

    @JsonCreator
    public SessionRepresentation(
        @JsonProperty("traceId") String traceId,
        @JsonProperty("catalog") String catalog,
        @JsonProperty("schema") String schema,
        @JsonProperty("user") String user,
        @JsonProperty("host") String host,
        @JsonProperty("encoding") String encoding,
        @JsonProperty("mdcConnString") String mdcConnString,
        @JsonProperty("sqlMode") String sqlMode,
        @JsonProperty("txIsolation") int txIsolation,
        @JsonProperty("socketTimeout") int socketTimeout,
        @JsonProperty("enableTrace") boolean enableTrace,
        @JsonProperty("queryStart") long queryStart,
        @JsonProperty("serverVariables") Map<String, Object> serverVariables,
        @JsonProperty("userDefVariables") Map<String, Object> userDefVariables,
        @JsonProperty("hintCmds") Map<String, Object> hintCmds,
        @JsonProperty("params") Map<Integer, ParameterContext> params,
        @JsonProperty("cacheRelNodesId") Set<Integer> cacheRelNodesId,
        @JsonProperty("recordRowCnt") Map<Integer, Integer> recordRowCnt,
        @JsonProperty("distinctKeyCnt") Map<Integer, Integer> distinctKeyCnt,
        @JsonProperty("testMode") boolean testMode,
        @JsonProperty("lastInsertId") long lastInsertId,
        @JsonProperty("logicalTimeZone") InternalTimeZone logicalTimeZone,
        @JsonProperty("tsoTimeStamp") long tsoTimeStamp,
        @JsonProperty("useColumnar") boolean useColumnar,
        @JsonProperty("dnLsnMap") Map<String, Long> dnLsnMap,
        @JsonProperty("omitTso") boolean omitTso,
        @JsonProperty("lizard1PC") boolean lizard1PC,
        @JsonProperty("useColumnarTracer") boolean useColumnarTracer,
        @JsonProperty("workloadType") WorkloadType workloadType,
        @JsonProperty("extraServerVariables") Map<String, Object> extraServerVariables) {
        this.traceId = traceId;
        this.catalog = catalog;
        this.schema = schema;
        this.user = user;
        this.host = host;
        this.encoding = encoding;
        this.mdcConnString = mdcConnString;
        this.sqlMode = sqlMode;
        this.txIsolation = txIsolation;
        this.socketTimeout = socketTimeout;
        this.enableTrace = enableTrace;
        this.queryStart = queryStart;
        this.serverVariables = serverVariables;
        this.userDefVariables = userDefVariables;
        this.hintCmds = hintCmds;
        this.params = params;
        this.cacheRelNodesId = cacheRelNodesId;
        this.recordRowCnt = recordRowCnt;
        this.distinctKeyCnt = distinctKeyCnt;
        this.testMode = testMode;
        this.lastInsertId = lastInsertId;
        this.logicalTimeZone = logicalTimeZone;
        this.tsoTimeStamp = tsoTimeStamp;
        this.useColumnar = useColumnar;
        this.dnLsnMap = dnLsnMap;
        this.omitTso = omitTso;
        this.lizard1PC = lizard1PC;
        this.useColumnarTracer = useColumnarTracer;
        this.workloadType = workloadType;
        this.extraServerVariables = extraServerVariables;
    }

    public SessionRepresentation(
        String traceId,
        String catalog,
        String schema,
        String user,
        String host,
        String encoding,
        String mdcConnString,
        String sqlMode,
        int txIsolation,
        int socketTimeout,
        boolean enableTrace,
        long queryStart,
        Map<String, Object> serverVariables,
        Map<String, Object> userDefVariables,
        Map<String, Object> hintCmds,
        Parameters params,
        Set<Integer> cacheRelNodesId,
        Map<Integer, Integer> recordRowCnt,
        Map<Integer, Integer> distinctKeyCnt,
        boolean testMode,
        long lastInsertId,
        InternalTimeZone logicalTimeZone,
        long tsoTimeStamp,
        boolean useColumnar,
        Map<String, Long> dnLsnMap,
        boolean omitTso,
        boolean lizard1PC,
        boolean useColumnarTracer,
        WorkloadType workloadType,
        Map<String, Object> extraServerVariables) {
        this.traceId = traceId;
        this.catalog = catalog;
        this.schema = schema;
        this.user = user;
        this.host = host;
        this.encoding = encoding;
        this.mdcConnString = mdcConnString;
        this.sqlMode = sqlMode;
        this.txIsolation = txIsolation;
        this.socketTimeout = socketTimeout;
        this.enableTrace = enableTrace;
        this.queryStart = queryStart;
        this.serverVariables = serverVariables;
        this.userDefVariables = userDefVariables;
        this.hintCmds = hintCmds;
        this.params = params.getCurrentParameter();
        this.cacheRelNodesId = cacheRelNodesId;
        this.recordRowCnt = recordRowCnt;
        this.distinctKeyCnt = distinctKeyCnt;
        this.testMode = testMode;
        this.lastInsertId = lastInsertId;
        this.logicalTimeZone = logicalTimeZone;
        this.tsoTimeStamp = tsoTimeStamp;
        this.useColumnar = useColumnar;
        this.dnLsnMap = dnLsnMap;
        this.workloadType = workloadType;
        this.omitTso = omitTso;
        this.lizard1PC = lizard1PC;
        this.useColumnarTracer = useColumnarTracer;
        this.extraServerVariables = extraServerVariables;
    }

    @JsonProperty
    public Set<Integer> getCacheRelNodesId() {
        return cacheRelNodesId;
    }

    @JsonProperty
    public Map<Integer, Integer> getRecordRowCnt() {
        return recordRowCnt;
    }

    @JsonProperty
    public Map<Integer, Integer> getDistinctKeyCnt() {
        return distinctKeyCnt;
    }

    @JsonProperty
    public long getQueryStart() {
        return queryStart;
    }

    @JsonProperty
    public String getTraceId() {
        return traceId;
    }

    @JsonProperty
    public String getCatalog() {
        return catalog;
    }

    @JsonProperty
    public String getSchema() {
        return schema;
    }

    @JsonProperty
    public String getUser() {
        return user;
    }

    @JsonProperty
    public String getHost() {
        return host;
    }

    @JsonProperty
    public String getEncoding() {
        return encoding;
    }

    @JsonProperty
    public String getMdcConnString() {
        return mdcConnString;
    }

    @JsonProperty
    public String getSqlMode() {
        return sqlMode;
    }

    @JsonProperty
    public int getTxIsolation() {
        return txIsolation;
    }

    @JsonProperty
    public int getSocketTimeout() {
        return socketTimeout;
    }

    @JsonProperty
    public boolean isEnableTrace() {
        return enableTrace;
    }

    @JsonProperty
    public Map<String, Object> getServerVariables() {
        return serverVariables;
    }

    @JsonProperty
    public Map<String, Object> getUserDefVariables() {
        return userDefVariables;
    }

    @JsonProperty
    public Map<String, Object> getHintCmds() {
        return hintCmds;
    }

    @JsonProperty
    public Map<Integer, ParameterContext> getParams() {
        return params;
    }

    @JsonProperty
    public boolean isTestMode() {
        return testMode;
    }

    @JsonProperty
    public long getLastInsertId() {
        return lastInsertId;
    }

    @JsonProperty
    public InternalTimeZone getLogicalTimeZone() {
        return logicalTimeZone;
    }

    @JsonProperty
    public long getTsoTimeStamp() {
        return tsoTimeStamp;
    }

    @JsonProperty
    public boolean isUseColumnar() {
        return useColumnar;
    }

    @JsonProperty
    public Map<String, Long> getDnLsnMap() {
        return dnLsnMap;
    }

    @JsonProperty
    public WorkloadType getWorkloadType() {
        return workloadType;
    }

    @JsonProperty
    public boolean isOmitTso() {
        return omitTso;
    }

    @JsonProperty
    public boolean isLizard1PC() {
        return lizard1PC;
    }

    @JsonProperty
    public Map<String, Object> getExtraServerVariables() {
        return extraServerVariables;
    }

    @JsonProperty
    public boolean getUseColumnarTracer() {
        return useColumnarTracer;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("queryStart", queryStart)
            .add("traceId", traceId)
            .add("catalog", catalog)
            .add("schema", schema)
            .add("encoding", encoding)
            .add("sqlMode", sqlMode)
            .add("enableTrace", enableTrace)
            .add("hintCmds", hintCmds)
            .toString();
    }

    public Session toSession(TaskId taskId, QueryContext queryContext, long trxId, ColumnarTracer columnarTracer) {
        ExecutionContext ec = TaskResource.getDrdsContextHandler().makeExecutionContext(schema, hintCmds, txIsolation);
        ec.setTxId(trxId);
        if (tsoTimeStamp > 0 || omitTso) {
            ITransactionManager tm = ExecutorContext.getContext(schema).getTransactionManager();

            if (useColumnar) {
                IColumnarTransaction transaction = (IColumnarTransaction) tm.createTransaction(
                    ITransactionPolicy.TransactionClass.COLUMNAR_READ_ONLY_TRANSACTION, ec);
                transaction.setTsoTimestamp(tsoTimeStamp);
                ec.setTransaction(transaction);
            } else {
                IMppReadOnlyTransaction transaction = (IMppReadOnlyTransaction) tm.createTransaction(
                    ITransactionPolicy.TransactionClass.MPP_READ_ONLY_TRANSACTION, ec);
                transaction.setSnapshotTimestamp(tsoTimeStamp);
                transaction.setDnLsnMap(dnLsnMap);
                transaction.enableOmitTso(omitTso, lizard1PC);
                ec.setTransaction(transaction);
            }

            ec.setAutoCommit(true);
        } else {
            ITransaction transaction = ExecutorContext.getContext(schema).getTransactionManager().createTransaction(
                ITransactionPolicy.TransactionClass.AUTO_COMMIT, ec);
            ec.setTransaction(transaction);
            ec.setAutoCommit(true);
        }

        ec.setAppName(catalog);
        //FIXME ec.setMppMode(MppMode.XXXX);
        ec.setTraceId(traceId);
        // phySqlId is ignorable for SELECT
        ec.setPhySqlId(0L);
        ec.setEncoding(encoding);
        ec.setMdcConnString(mdcConnString);
        ec.setSqlMode(sqlMode);
        ec.setTxIsolation(txIsolation);
        ec.setSocketTimeout(socketTimeout);
        ec.setEnableTrace(enableTrace);
        if (enableTrace) {
            ec.setTracer(new SQLTracer());
            ec.setColumnarTracer(columnarTracer);
        }
        ec.setServerVariables(serverVariables);
        ec.setUserDefVariables(userDefVariables);
        if (params != null) {
            ec.setParams(new Parameters(params));
        }
        ec.getCacheRelNodeIds().addAll(cacheRelNodesId);
        ec.getRecordRowCnt().putAll(recordRowCnt);
        if (distinctKeyCnt != null) {
            ec.getDistinctKeyCnt().putAll(distinctKeyCnt);
        }
        ec.setInternalSystemSql(false);
        ec.setUsingPhySqlCache(true);

        //mock connection
        MppMockConnection mppMockConnection = new MppMockConnection(user);
        mppMockConnection.setLastInsertId(lastInsertId);

        ec.setConnection(mppMockConnection);
        ec.setTimeZone(logicalTimeZone);
        ec.setWorkloadType(workloadType);
        //server端的内存限制遵循TConnection的执行，worker端使用mpp的限制
        synchronized (queryContext) {
            QueryMemoryPool queryMemoryPool = queryContext.getQueryMemoryPool();
            if (queryMemoryPool == null) {
                queryMemoryPool = queryContext.createQueryMemoryPool(schema, ec);
            }

            QuerySpillSpaceMonitor monitor = queryContext.getQuerySpillSpaceMonitor();
            if (monitor == null) {
                monitor = queryContext.createQuerySpillSpaceMonitor();
            }

            //使用TASK级MemoryPool作为task调度端ExecutionContext的QueryMemoryPool
            ec.setMemoryPool(
                queryMemoryPool.getOrCreatePool(taskId.toString(), queryMemoryPool.getMaxLimit(), MemoryType.TASK));

            ec.setQuerySpillSpaceMonitor(monitor);
        }
        ec.setClientIp(host);
        if (ec.getPrivilegeContext() != null) {
            PrivilegeContext privilegeContext = ec.getPrivilegeContext();
            privilegeContext.setHost(host);
            privilegeContext.setSchema(schema);
            privilegeContext.setUser(user);
            privilegeContext.setManaged(false);
        }
        ec.setTestMode(testMode);
        ec.setExtraServerVariables(extraServerVariables);

        return new Session(taskId.getStageId(), ec);
    }
}
