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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.LogicalShowProfileHandler;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.optimizer.ccl.common.CclContext;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;
import com.alibaba.polardbx.optimizer.ccl.common.RescheduleTask;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.conn.InnerConnection;
import com.alibaba.polardbx.server.conn.InnerConnectionManager;
import com.alibaba.polardbx.server.handler.pl.RuntimeProcedureManager;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import org.apache.calcite.plan.RelOptCost;

import java.text.DecimalFormat;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

/**
 * @author 梦实 2017年9月11日 下午4:19:18
 * @since 5.0.0
 */
public class ShowProcesslistSyncAction implements ISyncAction {
    protected static final DecimalFormat NUM_FORMAT = new DecimalFormat("#,###");
    private String db;
    private boolean full;

    public ShowProcesslistSyncAction() {
    }

    public ShowProcesslistSyncAction(boolean full) {
        this.full = full;
    }

    public ShowProcesslistSyncAction(String db, boolean full) {
        this.db = db;
        this.full = full;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public boolean isFull() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }

    @Override
    public ResultCursor sync() {

        ArrayResultCursor result = new ArrayResultCursor("processlist");
        result.addColumn("Id", DataTypes.LongType);
        result.addColumn("User", DataTypes.StringType);
        result.addColumn("Host", DataTypes.StringType);
        result.addColumn("db", DataTypes.StringType);
        result.addColumn("Command", DataTypes.StringType);
        result.addColumn("Time", DataTypes.LongType);
        result.addColumn("CpuTime(ns)", DataTypes.StringType);
        result.addColumn("Memory(byte)", DataTypes.StringType);
        result.addColumn("MemPct", DataTypes.StringType);
        result.addColumn("State", DataTypes.StringType);
        result.addColumn("Info", DataTypes.StringType);
        result.addColumn("SQL_TEMPLATE_ID", DataTypes.StringType);
        result.addColumn("SQL_TYPE", DataTypes.StringType);
        result.addColumn("CPU", DataTypes.DoubleType);
        result.addColumn("MEMORY", DataTypes.DoubleType);
        result.addColumn("IO", DataTypes.DoubleType);
        result.addColumn("NET", DataTypes.DoubleType);
        result.addColumn("TYPE", DataTypes.StringType);
        result.addColumn("Route", DataTypes.StringType);
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("TraceId", DataTypes.StringType);

        result.initMeta();

        for (NIOProcessor p : CobarServer.getInstance().getProcessors()) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc instanceof ServerConnection) {
                    addRowByConnection(result, (ServerConnection) fc);
                }
            }
        }

        for (Map.Entry<Long, InnerConnection> conn : InnerConnectionManager.getActiveConnections().entrySet()) {
            addRowByInnerConnection(result, conn.getValue());
        }

        return result;
    }

    private void addRowByConnection(ArrayResultCursor result, ServerConnection sc) {
        String command = "SLEEP";
        String info = null;
        String sqlTid = null;
        String sqlType = null;
        boolean isStmtExecuting = sc.isStatementExecuting().get();
        if (isStmtExecuting) {
            command = "Query";
            if (this.isFull()) {
                info = sc.getSqlSample();

                TConnection tConnection = sc.getTddlConnection();
                if (tConnection != null) {
                    ExecutionContext executionContext = tConnection.getExecutionContext();
                    if (executionContext != null) {
                        SqlType executionSqlType = executionContext.getSqlType();
                        if (executionSqlType != null) {
                            sqlType = executionSqlType.toString();
                        }
                        ExecutionPlan plan = executionContext.getFinalPlan();
                        if (plan != null) {
                            //use the first plan. consider their ther PlanWithContext is similar when InsertSplitter is applied.

                            PlanCache.CacheKey cacheKey = plan.getCacheKey();
                            if (cacheKey != null) {
                                sqlTid = cacheKey.getTemplateId();
                            }
                        }
                    }
                }
            } else {
                info = TStringUtil.substring(sc.getSqlSample(), 0, 30);
            }
            TConnection tConnection = sc.getTddlConnection();
            if (tConnection != null) {
                ExecutionContext executionContext = tConnection.getExecutionContext();
                if (executionContext != null) {
                    CclContext cclContext = executionContext.getCclContext();
                    if (cclContext != null) {
                        Thread thread = cclContext.getThread();
                        Object blocker = LockSupport.getBlocker(thread);
                        if (blocker instanceof CclRuleInfo) {
                            String ruleName = ((CclRuleInfo) blocker).getCclRuleRecord().id;
                            command = "Query(Waiting-" + ruleName + ")";
                        }
                    }
                }
            }

        }

        if (executingProcedure(sc.getId())) {
            command = "Query";
            info = addProcedureInfo(sc.getId(), info);
        }

        if (sc.getProxy() != null) {
            command = "Binlog Dump";
            info = "Sending to client";
        }

        long time = (System.nanoTime() - sc.getLastActiveTime()) / 1000000000;

        long allTc = 0;
        long mem = 0;
        String allTcStr = LogicalShowProfileHandler.PROFILE_ZEOR_VALUE;
        String memStr = LogicalShowProfileHandler.PROFILE_NO_VALUE;
        String memPctStr = LogicalShowProfileHandler.PROFILE_NO_VALUE;
        TConnection tConn = sc.getTddlConnection();
        RelOptCost cost = DrdsRelOptCostImpl.FACTORY.makeZeroCost();
        WorkloadType workloadType = null;
        String route = null;
        String worker = null;
        if (info != null) {
            workloadType = WorkloadType.TP;
            route = ConfigDataMode.isReadOnlyMode() ? "RO" : "RW";
            worker = ServiceProvider.getInstance().getServer().getLocalNode().getHostMppPort();
        }

        if (tConn != null && isStmtExecuting) {
            ExecutionContext ec = tConn.getExecutionContext();
            if (ec != null) {
                RuntimeStatistics runTimeStat = (RuntimeStatistics) ec.getRuntimeStatistics();
                if (runTimeStat != null && ExecUtils.isOperatorMetricEnabled(ec)) {
                    allTc = runTimeStat.getSqlCpuTime();
                    allTcStr = NUM_FORMAT.format(allTc);
                    mem = runTimeStat.getSqlMemoryMaxUsage();
                    if (mem > 0) {
                        memStr = NUM_FORMAT.format(mem);
                    } else {
                        memStr = LogicalShowProfileHandler.PROFILE_NO_VALUE;
                    }
                    double memPct = runTimeStat.getSqlMemoryMaxUsagePct();
                    if (memPct > 0 && mem > 0) {
                        memPctStr = String.format("%.4f%%", memPct);
                    } else {
                        memPctStr = LogicalShowProfileHandler.PROFILE_NO_VALUE;
                    }
                }
                cost = CBOUtil.getCost(ec);
                ExplainResult explainResult = ec.getExplain();
                workloadType = WorkloadUtil.getWorkloadType(ec);
                if (explainResult != null && !explainResult.explainMode.isAnalyze()) {
                    workloadType = WorkloadType.TP;
                }
                if (ConfigDataMode.isMasterMode()) {
                    route = !ExecUtils.isMppMode(ec) ? "RW" : "RO";
                }

                try {
                    worker = String.join(",", ExecUtils.getQuerySchedulerHosts(ec));
                } catch (Throwable t) {
                    worker = null;
                }
            }
        }

        //ccl reschedule
        RescheduleTask rescheduleTask = sc.getRescheduleTask();
        if (sc.isRescheduled() && rescheduleTask != null) {
            if (rescheduleTask.isSwitchoverReschedule()) {
                if (isStmtExecuting) {
                    command = "Query(Rescheduling-switchover)";
                } else {
                    command = "SLEEP(Rescheduling-switchover)";
                    if (this.isFull()) {
                        info = sc.getSqlSample();
                    } else {
                        info = TStringUtil.substring(sc.getSqlSample(), 0, 30);
                    }
                }
            } else {
                String ruleName = rescheduleTask.getCclRuleInfo().getCclRuleRecord().id;
                if (isStmtExecuting) {
                    command = "Query(Rescheduling-" + ruleName + ")";
                } else {
                    command = "SLEEP(Rescheduling-" + ruleName + ")";
                    if (this.isFull()) {
                        info = sc.getSqlSample();
                    } else {
                        info = TStringUtil.substring(sc.getSqlSample(), 0, 30);
                    }
                }
            }
        }
        result.addRow(new Object[] {
            sc.getId(), sc.getUser(), sc.getHost() + ":" + sc.getPort(),
            sc.getSchema(), command, time, allTcStr, memStr, memPctStr, "", info, sqlTid, sqlType,
            cost.getCpu(), cost.getMemory(), cost.getIo(), cost.getNet(),
            workloadType, route, worker, sc.getTraceId()});

    }

    private void addRowByInnerConnection(ArrayResultCursor result, InnerConnection innerConnection) {
        String command = "INNER-SLEEP";
        String info = null;
        String sqlTid = null;
        String sqlType = null;
        boolean isStmtExecuting = innerConnection.isStatementExecuting();
        if (isStmtExecuting) {
            command = "Query";
            if (this.isFull()) {
                info = innerConnection.getSqlSample();

                TConnection tConnection = innerConnection.getTConnection();
                if (tConnection != null) {
                    ExecutionContext executionContext = tConnection.getExecutionContext();
                    if (executionContext != null) {
                        SqlType executionSqlType = executionContext.getSqlType();
                        if (executionSqlType != null) {
                            sqlType = executionSqlType.toString();
                        }
                        ExecutionPlan plan = executionContext.getFinalPlan();
                        if (plan != null) {
                            PlanCache.CacheKey cacheKey = plan.getCacheKey();
                            if (cacheKey != null) {
                                sqlTid = cacheKey.getTemplateId();
                            }
                        }
                    }
                }
            } else {
                info = TStringUtil.substring(innerConnection.getSqlSample(), 0, 30);
            }
        }

        // in second.
        long time = (System.nanoTime() - innerConnection.getLastActiveTime()) / 1_000_000_000;

        long allTc = 0;
        long mem = 0;
        String allTcStr = LogicalShowProfileHandler.PROFILE_ZEOR_VALUE;
        String memStr = LogicalShowProfileHandler.PROFILE_NO_VALUE;
        String memPctStr = LogicalShowProfileHandler.PROFILE_NO_VALUE;
        TConnection tConn = innerConnection.getTConnection();
        RelOptCost cost = DrdsRelOptCostImpl.FACTORY.makeZeroCost();
        WorkloadType workloadType = null;
        String route = null;
        String worker = null;
        if (info != null) {
            workloadType = WorkloadType.TP;
            route = ConfigDataMode.isReadOnlyMode() ? "RO" : "RW";
            worker = ServiceProvider.getInstance().getServer().getLocalNode().getHostMppPort();
        }

        if (tConn != null && isStmtExecuting) {
            ExecutionContext ec = tConn.getExecutionContext();
            if (ec != null) {
                RuntimeStatistics runTimeStat = (RuntimeStatistics) ec.getRuntimeStatistics();
                if (runTimeStat != null && ExecUtils.isOperatorMetricEnabled(ec)) {
                    allTc = runTimeStat.getSqlCpuTime();
                    allTcStr = NUM_FORMAT.format(allTc);
                    mem = runTimeStat.getSqlMemoryMaxUsage();
                    if (mem > 0) {
                        memStr = NUM_FORMAT.format(mem);
                    } else {
                        memStr = LogicalShowProfileHandler.PROFILE_NO_VALUE;
                    }
                    double memPct = runTimeStat.getSqlMemoryMaxUsagePct();
                    if (memPct > 0 && mem > 0) {
                        memPctStr = String.format("%.4f%%", memPct);
                    } else {
                        memPctStr = LogicalShowProfileHandler.PROFILE_NO_VALUE;
                    }
                }
                cost = CBOUtil.getCost(ec);
                ExplainResult explainResult = ec.getExplain();
                workloadType = WorkloadUtil.getWorkloadType(ec);
                if (explainResult != null && !explainResult.explainMode.isAnalyze()) {
                    workloadType = WorkloadType.TP;
                }
                if (ConfigDataMode.isMasterMode()) {
                    route = !ExecUtils.isMppMode(ec) ? "RW" : "RO";
                }

                try {
                    worker = String.join(",", ExecUtils.getQuerySchedulerHosts(ec));
                } catch (Throwable t) {
                    worker = null;
                }
            }
        }

        result.addRow(new Object[] {
            innerConnection.getId(), innerConnection.getUser(), "127.0.0.1:1111",
            innerConnection.getSchemaName(), command, time, allTcStr, memStr, memPctStr, "", info, sqlTid, sqlType,
            cost.getCpu(), cost.getMemory(), cost.getIo(), cost.getNet(),
            workloadType, route, worker, innerConnection.getTraceId()});
    }

    private boolean executingProcedure(long connId) {
        return RuntimeProcedureManager.getInstance().search(connId) != null;
    }

    private String addProcedureInfo(long connId, String info) {
        if (info == null || "NULL".equalsIgnoreCase(info)) {
            info = "executing pl logic";
        }
        return "CALL " + RuntimeProcedureManager.getInstance().search(connId).getName() + ": " + info;
    }

}
