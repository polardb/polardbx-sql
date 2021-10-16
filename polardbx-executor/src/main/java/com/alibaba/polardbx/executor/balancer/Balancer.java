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

package com.alibaba.polardbx.executor.balancer;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.executor.balancer.action.ActionUtils;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.balancer.policy.BalancePolicy;
import com.alibaba.polardbx.executor.balancer.policy.PolicyDataBalance;
import com.alibaba.polardbx.executor.balancer.policy.PolicyDrainNode;
import com.alibaba.polardbx.executor.balancer.policy.PolicyMergePartition;
import com.alibaba.polardbx.executor.balancer.splitpartition.PolicySplitPartition;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.GroupStats;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.balancer.stats.TableGroupStat;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlRebalance;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Balancer that make the cluster balanced through data migration
 *
 * @author moyi
 * @since 2021/03
 */
public class Balancer extends AbstractLifecycle {

    private static final Balancer INSTANCE = new Balancer();
    private static final int NUM_BALANCER_THREADS = 1;

    private static Logger LOG = SQLRecorderLogger.ddlLogger;

    /**
     * Supported policies
     */
    private final ImmutableMap<String, BalancePolicy> policyMap =
        new ImmutableMap.Builder<String, BalancePolicy>()
            .put(SqlRebalance.POLICY_SPLIT_PARTITION, new PolicySplitPartition())
            .put(SqlRebalance.POLICY_MERGE_PARTITION, new PolicyMergePartition())
            .put(SqlRebalance.POLICY_DRAIN_NODE, new PolicyDrainNode())
            .put(SqlRebalance.POLICY_DATA_BALANCE, new PolicyDataBalance())
            .build();

    // Options
    private volatile boolean enableBalancer = false;
    private BalancerThread balancerThread;
    private ExecutorService schedulerPool;

    // Time window which allows running background balance
    private DateTime balanceWindowStart;
    private DateTime balanceWindowEnd;

    private Balancer() {
    }

    public static Balancer getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }

        return INSTANCE;
    }

    @Override
    public void doInit() {
        LOG.info("BalanceScheduler initialized");
        this.schedulerPool = ExecutorUtil.createExecutor("Balancer", NUM_BALANCER_THREADS);
        this.balancerThread = new BalancerThread();
        this.schedulerPool.submit(this.balancerThread);
    }

    public boolean isEnabled() {
        return this.enableBalancer;
    }

    public void enableBalancer(boolean enable) {
        this.enableBalancer = enable;
    }

    /**
     * Example:
     * - 02:00-04:00
     */
    public void setBalancerRunningWindow(String window) {
        if (TStringUtil.isBlank(window)) {
            return;
        }
        String[] slices = window.split("-");
        if (slices.length != 2) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "Illegal time window " + window);
        }
        DateTime start = DateTime.parse(slices[0]);
        DateTime end = DateTime.parse(slices[1]);
        this.balanceWindowStart = start;
        this.balanceWindowEnd = end;
        LOG.info(String.format("set balancer running window to [%s,%s]", start, end));
    }

    public Pair<DateTime, DateTime> getBalancerWindow() {
        return Pair.of(this.balanceWindowStart, this.balanceWindowEnd);
    }

    /**
     * Rebalance Cluster: balance storage-nodes in cluster
     * Collect stats of all groups, try to balance group count among storage-node through move group between
     */
    public List<BalanceAction> rebalanceCluster(ExecutionContext ec, BalanceOptions options) {
        DdlJobManager jobManager = new DdlJobManager();
        String name = ActionUtils.genRebalanceClusterName();
        boolean ok = jobManager.getResourceManager().checkResource(Sets.newHashSet(), Sets.newHashSet(name));
        if (!ok) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "already in rebalance");
        }

        List<BalancePolicy> policies = getBalancePolicy(options.policy);
        List<DbInfoRecord> dbInfoList = DbInfoManager.getInstance().getDbInfoList();
        List<String> schemaList = dbInfoList.stream()
            .filter(DbInfoRecord::isUserDb).map(x -> x.dbName).collect(Collectors.toList());

        Map<String, BalanceStats> stats = schemaList.stream().map(schema ->
            collectBalanceStatsOfDatabase(ec, schema)
        ).collect(Collectors.toMap(BalanceStats::getSchema, x -> x));

        List<BalanceAction> actions = new ArrayList<>();

        for (BalancePolicy policy : policies) {
            actions.addAll(policy.applyToMultiDb(ec, stats, options, schemaList));
        }

        return actions;
    }

    /**
     * Rebalance Table: apply rebalance-policy on a specified table
     */
    public List<BalanceAction> rebalanceTable(ExecutionContext ec, String tableName, BalanceOptions options) {
        String schema = ec.getSchemaName();
        boolean isSharding = !DbInfoManager.getInstance().isNewPartitionDb(schema);
        if (isSharding) {
            throw new TddlRuntimeException(ErrorCode.ERR_REBALANCE, "only partition database support rebalance table");
        }
        BalanceStats stats = collectBalanceStatsOfTable(ec, schema, tableName);

        return rebalanceImpl(ec, options, stats, schema);
    }

    /**
     * Rebalance Database: apply all policies on all tables of current database
     */
    public List<BalanceAction> rebalanceDatabase(ExecutionContext ec, BalanceOptions options) {
        String schema = ec.getSchemaName();
        DdlJobManager jobManager = new DdlJobManager();
        String name = ActionUtils.genRebalanceResourceName(SqlRebalance.RebalanceTarget.DATABASE, schema);
        boolean ok = jobManager.getResourceManager().checkResource(Sets.newHashSet(), Sets.newHashSet(name));
        if (!ok) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "already in rebalance");
        }

        BalanceStats stats = collectBalanceStatsOfDatabase(ec, schema);

        return rebalanceImpl(ec, options, stats, schema);
    }

    private List<BalanceAction> rebalanceImpl(ExecutionContext ec,
                                              BalanceOptions options,
                                              BalanceStats stats,
                                              String schema) {
        List<BalancePolicy> policies = getBalancePolicy(options.policy);
        if (policies.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_REBALANCE, "Policy not found");
        }

        List<BalanceAction> actions = new ArrayList<>();
        for (BalancePolicy policy : policies) {
            actions.addAll(policy.applyToDb(ec, stats, options, schema));
        }
        return actions;
    }

    private List<BalancePolicy> getBalancePolicy(String policy) {
        if (TStringUtil.isBlank(policy)) {
            return Arrays.asList(getDefaultPolicy());
        }

        List<BalancePolicy> result = new ArrayList<>();
        for (String str : policy.split(",")) {
            BalancePolicy p = this.policyMap.get(str);
            if (p == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                    String.format("balance policy %s not found", str));
            }
            result.add(p);
        }

        return result;
    }

    private BalancePolicy getDefaultPolicy() {
        return new PolicyDataBalance();
    }

    private BalanceStats collectBalanceStatsOfTable(ExecutionContext ec, String schema, String tableName) {
        if (DbInfoManager.getInstance().isNewPartitionDb(schema)) {
            List<TableGroupStat> tableGroupStats = StatsUtils.getTableGroupsStats(ec.getSchemaName(), tableName);
            return BalanceStats.createForPartition(schema, tableGroupStats);
        } else {
            List<GroupStats.GroupsOfStorage> groupStats = GroupStats.getGroupsOfDb(schema);
            return BalanceStats.createForSharding(schema, groupStats);
        }
    }

    private BalanceStats collectBalanceStatsOfDatabase(ExecutionContext ec, String schema) {
        return collectBalanceStatsOfTable(ec, schema, null);
    }

}
