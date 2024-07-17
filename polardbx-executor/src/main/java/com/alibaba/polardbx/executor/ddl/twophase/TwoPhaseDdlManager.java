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

package com.alibaba.polardbx.executor.ddl.twophase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.ddl.workqueue.PriorityFIFOTask;
import com.alibaba.polardbx.executor.ddl.workqueue.TwoPhaseDdlThreadPool;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Pair;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionParams.CHECK_PHY_CONN_NUM;
import static com.alibaba.polardbx.common.properties.ConnectionParams.EMIT_PHY_TABLE_DDL_DELAY;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlData.REACHED_BARRIER_STATE;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlData.resultsToFinishSuccess;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlData.resultsToPhysicalDdlPhase;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlData.resultsToPhysicalDdlStateMap;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlData.resultsToQueryIdMap;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlData.resultsToQueryIdToProcessInfoMap;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlData.resultsToStateMap;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.COMMIT_STATE;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.PHYSICAL_DDL_EMIT_TASK_NAME;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.SQL_COMMIT_TWO_PHASE_DDL;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.SQL_FINISH_TWO_PHASE_DDL;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.SQL_INIT_TWO_PHASE_DDL;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.SQL_KILL_QUERY;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.SQL_PREPARE_TWO_PHASE_DDL;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.SQL_ROLLBACK_TWO_PHASE_DDL;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.SQL_SHOW_PROCESS_LIST;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.SQL_STATS_TWO_PHASE_DDL;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.SQL_WAIT_TWO_PHASE_DDL;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.TWO_PHASE_DDL_COMMIT_TASK_NAME;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.TWO_PHASE_DDL_FINISH_TASK_NAME;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.TWO_PHASE_DDL_INIT_TASK_NAME;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.TWO_PHASE_DDL_PREPARE_TASK_NAME;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.TWO_PHASE_DDL_ROLLBACK_TASK_NAME;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.TWO_PHASE_DDL_STATS;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.TWO_PHASE_PHYSICAL_DDL_HINT_TEMPLATE;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.WAIT_ROLLBACK_STATE;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.buildPhyDbTableNameFromGroupNameAndPhyTableName;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.buildTwoPhaseKeyFromLogicalTableNameAndGroupName;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.executePhyDdlBypassConnPool;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.formatString;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.queryGroup;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.queryGroupBypassConnPool;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.updateGroupBypassConnPool;
import static com.alibaba.polardbx.gms.util.GroupInfoUtil.buildPhysicalDbNameFromGroupName;

public class TwoPhaseDdlManager {
    //TODO: what if HA happen when rollback?
    private final static Logger LOG = SQLRecorderLogger.ddlLogger;
    private static final IdGenerator ID_GENERATOR = IdGenerator.getIdGenerator();

    public static int DEFAULT_WAIT_TIME_MS = 400;

    public static int FINISH_DDL_WAIT_TIME = 100;

    public static long EMIT_PHYSICAL_DDL_CHECK_INTERVAL_MS = 200L;

    public static Map<Long, TwoPhaseDdlManager> globalTwoPhaseDdlManagerMap = new ConcurrentHashMap<>();

    public Map<String, Long> queryIdMap = new ConcurrentHashMap<>();

    public Map<String, Map<String, String>> queryProcessInfoMap = new ConcurrentHashMap<>();

    public Map<String, Map<String, String>> queryPhyDdlRunningStateMap = new ConcurrentHashMap<>();

    public Map<String, String> queryPhyDdlPhaseMap = new ConcurrentHashMap<>();

    private final String schemaName;

    private final String tableName;

    private final Long twoPhaseDdlManagerId;

    private Long jobId = -1L;

    List<Exception> phyDdlExceps = new CopyOnWriteArrayList<>();

    List<FutureTask<Void>> phyDdlTasks = new ArrayList<>(0);

    public ConcurrentHashMap<String, Set<String>> phyTableEmitted = null;

    public ConcurrentHashMap<String, String> lastEmitPhyTableNameMap = null;

    public ConcurrentHashMap<String, Queue<Pair<String, FutureTask<Void>>>> phyDdlTaskGroupByGroupName = null;

    String phyDdlStmt;

    // physical db => list of physical table
    Map<String, Set<String>> sourcePhyTableNames;

    public TwoPhaseDdlReporter twoPhaseDdlReporter;

    AtomicReference<RunningState> runningState = new AtomicReference<>(RunningState.INIT);

    enum RunningState {
        INIT,
        PREPARE,
        COMMIT,
        ROLLBACK,
        FINISH,
        PAUSED

    }

    public static Boolean checkEnableTwoPhaseDdlOnDn(String schemaName, String logicalTableName,
                                                     ExecutionContext executionContext) {
        Map<String, String> sourceGroupDnMap =
            DnStats.buildGroupToDnMap(schemaName, logicalTableName, executionContext);
        Map<String, String> dnToGroupMap = new HashMap<>();
        for (String group : sourceGroupDnMap.keySet()) {
            dnToGroupMap.put(sourceGroupDnMap.get(group), group);
        }
        AtomicBoolean enableTwoPhaseDdlOnDn = new AtomicBoolean(true);
        dnToGroupMap.keySet().forEach(
            dn -> {
                String group = dnToGroupMap.get(dn);
                String sql = TwoPhaseDdlUtils.SQL_SHOW_VARIABLES_LIKE_ENABLE_TWO_PHASE_DDL;
                List<List<Object>> results =
                    queryGroup(executionContext, -1L, "preCheck", schemaName, logicalTableName, group, sql);
                if (!results.isEmpty()) {
                    if (results.get(0).get(1).toString().equalsIgnoreCase("OFF")) {
                        enableTwoPhaseDdlOnDn.set(false);
                    }
                } else {
                    enableTwoPhaseDdlOnDn.set(false);
                }
            }
        );
        return enableTwoPhaseDdlOnDn.get();
    }

    public static Long generateTwoPhaseDdlManagerId(String schemaName, String tableName) {
        Long id = ID_GENERATOR.nextId();
        // there would be problem if multiple cn receive job. so we need to generate global unique id from gms like sequence.
        while (!TwoPhaseDdlReporter.acquireTwoPhaseDdlId(schemaName, tableName, id)) {
            id = ID_GENERATOR.nextId();
        }
        return id;
    }

    public TwoPhaseDdlManager(String schemaName, String tableName, String phyDdlStmt,
                              Map<String, Set<String>> sourcePhyTableNames, Long twoPhaseDdlManagerId
    ) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.phyDdlStmt = phyDdlStmt;
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.twoPhaseDdlManagerId = twoPhaseDdlManagerId;
        this.runningState.set(RunningState.INIT);
        this.twoPhaseDdlReporter = new TwoPhaseDdlReporter();
        globalTwoPhaseDdlManagerMap.put(this.twoPhaseDdlManagerId, this);
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    Boolean checkThreadAndConnectionEnough(String schemaName, String logicalTableName,
                                           ExecutionContext executionContext) {
        Map<String, String> sourceGroupDnMap =
            DnStats.buildGroupToDnMap(schemaName, logicalTableName, executionContext);
        Map<String, DnStats> dnStateMap =
            DnStats.buildDnStats(schemaName, tableName, sourceGroupDnMap, jobId, executionContext);
        Map<String, Integer> requiredConnections = new HashMap<>();
        Boolean checkEnough = true;
        for (String sourceGroupName : sourcePhyTableNames.keySet()) {
            String dn = sourceGroupDnMap.get(sourceGroupName);
            requiredConnections.put(dn,
                requiredConnections.getOrDefault(dn, 0) + sourcePhyTableNames.get(sourceGroupName).size());
        }
        for (String dn : dnStateMap.keySet()) {
            if (requiredConnections.containsKey(dn) && !dnStateMap.get(dn)
                .checkConnectionNum(requiredConnections.get(dn))) {
                checkEnough = false;
            }
        }
        //if(check alive thread num ok){
        //
        //}
        return checkEnough;
    }

    public static Map<String, String> calPhyTableHashCodeMap(String schemaName,
                                                             Map<String, Set<String>> sourcePhyTableNames) {
        Map<String, String> phyTableHashCodeMap = new HashMap<>();
        sourcePhyTableNames.keySet().forEach(
            sourceGroupName -> {
                for (String phyTableName : sourcePhyTableNames.get(sourceGroupName)) {
                    String hashCode = DdlHelper.genHashCodeForPhyTableDDL(schemaName, sourceGroupName,
                        SqlIdentifier.surroundWithBacktick(phyTableName), 0);
                    String fullPhyTableName =
                        buildPhyDbTableNameFromGroupNameAndPhyTableName(sourceGroupName, phyTableName);
                    phyTableHashCodeMap.put(fullPhyTableName, hashCode);
                }
            }
        );
        return phyTableHashCodeMap;

    }

    /**
     * init two phase ddl for logical table
     */
    public void twoPhaseDdlInit(String logicalTableName,
                                ExecutionContext originEc) {
        if (sourcePhyTableNames == null || sourcePhyTableNames.isEmpty()) {
            return;
        } //TODO, report error when sourcePhyTableName error

        if (originEc.getParamManager().getBoolean(CHECK_PHY_CONN_NUM)) {
            if (!checkThreadAndConnectionEnough(schemaName, logicalTableName, originEc)) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "there are no enough connection or thread to alter table by consistency commit! you can use "
                        + "/*+TDDL:cmd_extra(ENABLE_DRDS_MULTI_PHASE_DDL=false)*/ to execute this ddl.  ");
            }
        }
        sourcePhyTableNames.forEach((sourceGroupName, phyTableNames) -> {
            //TODO implement initPhyTableDdl result process!
            initPhyTableDdl(
                schemaName,
                logicalTableName,
                sourceGroupName,
                phyTableNames,
                originEc
            );
        });
        this.runningState.set(RunningState.INIT);
    }

    // Return true when: not interrupted.
    // Return false when: interrupted
    public Boolean twoPhaseDdlEmit(String logicalTableName,
                                   ConcurrentHashMap<String, Set<String>> phyTableEmittedInTask,
                                   ExecutionContext originEc) throws InterruptedException {
        // 1. Initial some state(count), dict(sourceGroupToDnMap, dnToSourceGroupMap), parameter(concurrencyPolicy)
        AtomicInteger count = new AtomicInteger();
        Map<String, String> sourceGroupToDnMap = DnStats.buildGroupToDnMap(schemaName, logicalTableName, originEc);
        Map<String, List<String>> dnToSourceGroupMap = sourceGroupToDnMap
            .entrySet().stream().collect(Collectors.groupingBy(Map.Entry::getValue,
                Collectors.mapping(Map.Entry::getKey, Collectors.toList())));
        QueryConcurrencyPolicy concurrencyPolicy = getConcurrencyPolicy(originEc);
        // 2. Check jobInterrupted.
        Boolean jobInterrupted = CrossEngineValidator.isJobInterrupted(originEc);
        if (!jobInterrupted) {
            runningState.set(RunningState.INIT);
        }
        if (sourcePhyTableNames == null || sourcePhyTableNames.isEmpty()) {
            return true;
        }
        // 3. Reload more state(groupName => phyDdlTask Map, phyTableEmitted).
        if (phyDdlTaskGroupByGroupName == null) {
            phyDdlTaskGroupByGroupName = new ConcurrentHashMap<>();
        }

        if (this.phyTableEmitted == null) {
            phyTableEmitted = phyTableEmittedInTask;
        }
        if (this.phyTableEmitted == null) {
            phyTableEmitted = new ConcurrentHashMap<>();
        }
        // 4. Initial phyTableEmitted, if all emitted, return !jobInterrupted.
        if (checkAllPhyDdlEmited(logicalTableName, originEc)) {
            return !jobInterrupted;
        }
        Map mdcContext = MDC.getCopyOfContextMap();
        // 5. Loop groupName:
        for (String sourceGroupName : sourceGroupToDnMap.keySet()) {
            Set<String> phyTableNames = sourcePhyTableNames.getOrDefault(sourceGroupName
                , new HashSet<>());
            phyDdlTaskGroupByGroupName.put(sourceGroupName, new LinkedList<>());
            if (!phyTableEmitted.containsKey(sourceGroupName)) {
                phyTableEmitted.put(sourceGroupName, new HashSet<>());
            }
            //    5.2 secondly, if generate task for phy table not emitted.
            for (String phyTableName : phyTableNames) {
                if (!phyTableEmitted.get(sourceGroupName).contains(phyTableName) || !isPhysicalRunning(
                    phyTableName)) {
                    FutureTask<Void> task = new FutureTask<>(
                        () -> {
                            MDC.setContextMap(mdcContext);
                            emitPhyTableDdl(
                                logicalTableName,
                                PHYSICAL_DDL_EMIT_TASK_NAME,
                                sourceGroupName, phyTableName,
                                phyDdlStmt,
                                originEc,
                                phyDdlExceps,
                                count
                            );
                        }, null);
                    phyDdlTasks.add(task);
                    phyDdlTaskGroupByGroupName.get(sourceGroupName).add(Pair.of(phyTableName, task));
                    count.addAndGet(1);
                }
            }
        }

        // 6. Initialize phy table ddl state and last emit phy table...
        Boolean checkPhyDdlExceps = true;
        Map<String, Long> phyTableDdlStateMap = new HashMap<>();
        if (lastEmitPhyTableNameMap == null) {
            lastEmitPhyTableNameMap = new ConcurrentHashMap<>();
        }
        List<String> sourceGroupNames = new ArrayList<>(phyDdlTaskGroupByGroupName.keySet());
        // 7. Loop:
        while (count.get() > 0 && checkPhyDdlExceps && !jobInterrupted && inRunningState(runningState)) {
            //  7.1 for each group, collect stats into phyTableDdlStateMap.
            sourceGroupNames.forEach(sourceGroupName -> {
                String sql =
                    String.format(SQL_STATS_TWO_PHASE_DDL, schemaName,
                        buildTwoPhaseKeyFromLogicalTableNameAndGroupName(
                            logicalTableName, sourceGroupName
                        ));
                List<Map<String, Object>> results =
                    queryGroupBypassConnPool(originEc, jobId, PHYSICAL_DDL_EMIT_TASK_NAME, schemaName,
                        logicalTableName,
                        sourceGroupName,
                        sql);
                phyTableDdlStateMap.putAll(resultsToStateMap(results));
            });
            // 7.2 sort groups on dn by phyTableDdl task size
            dnToSourceGroupMap.keySet().forEach(dnInst ->
                dnToSourceGroupMap.get(dnInst)
                    .sort((o1, o2) -> phyDdlTaskGroupByGroupName.get(o1).size() - phyDdlTaskGroupByGroupName.get(o2)
                        .size()));

            // 7.3 for each group,
            sourceGroupNames.forEach(sourceGroupName -> {
                // 7.3.1 fetch remainPhyDdlTasks, if empty then remove group from remainPhyDdlTasks
                Long phyTableDdlState = REACHED_BARRIER_STATE;
                Queue<Pair<String, FutureTask<Void>>> remainPhyDdlTasks =
                    phyDdlTaskGroupByGroupName.get(sourceGroupName);
                // TODO: if state map contains no exceptions, then continue, otherwise just stop and rollback.
                String dnInst = sourceGroupToDnMap.get(sourceGroupName);
                if (remainPhyDdlTasks.isEmpty() && dnToSourceGroupMap.get(dnInst).contains(sourceGroupName)) {
                    dnToSourceGroupMap.get(dnInst).remove(sourceGroupName);
                }
                // 7.3.2 get last emit ddl state
                if (lastEmitPhyTableNameMap.containsKey(sourceGroupName)) {
                    String lastEmitPhyTableName = lastEmitPhyTableNameMap.get(sourceGroupName);
                    String fullPhyTableName =
                        buildPhyDbTableNameFromGroupNameAndPhyTableName(sourceGroupName, lastEmitPhyTableName);
                    phyTableDdlState = phyTableDdlStateMap.get(fullPhyTableName);
                }
                // 7.3.3 if state is ok & remain some physical ddl task. & concurrent, poll more task...
                if (phyTableDdlState == REACHED_BARRIER_STATE && !remainPhyDdlTasks.isEmpty()
                    || concurrencyPolicy == QueryConcurrencyPolicy.CONCURRENT) {
                    // only the first group in drds mode is considered in INSTANCE_CONCURRENT level
                    // and only when it's empty, will we do poll next physical table.
                    if (concurrencyPolicy == QueryConcurrencyPolicy.INSTANCE_CONCURRENT) {
                        if (!dnToSourceGroupMap.get(dnInst).isEmpty() && !dnToSourceGroupMap.get(dnInst).get(0)
                            .equalsIgnoreCase(sourceGroupName)) {
                            return;
                        }
                    }
                    int delay = originEc.getParamManager().getInt(EMIT_PHY_TABLE_DDL_DELAY);
                    if (delay > 0) {
                        try {
                            Thread.sleep(delay * 1000L);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    // poll a task from remainPhyDdlTasks, update last Emit task, decrement count.
                    // notice: update lastEmitTask in thread.
                    Pair<String, FutureTask<Void>> remainPhyDdlTask = remainPhyDdlTasks.poll();
                    String phyTableName = remainPhyDdlTask.getKey();
                    FutureTask<Void> task = remainPhyDdlTask.getValue();
                    Thread thread = new Thread(task);
                    thread.setName(String.format("MultiPhaseDdlThread_%s_%s", sourceGroupName, phyTableName));
                    thread.start();
                }
            });
            Thread.sleep(EMIT_PHYSICAL_DDL_CHECK_INTERVAL_MS);

            checkPhyDdlExceps = checkPhyDdlExcepsEmpty();
            jobInterrupted = CrossEngineValidator.isJobInterrupted(originEc);
        }

        while (checkPhyDdlExceps && !jobInterrupted && inRunningState(runningState)) {
            if (checkAllPhyDdlEmited(logicalTableName, originEc)) {
                break;
            }
            Thread.sleep(EMIT_PHYSICAL_DDL_CHECK_INTERVAL_MS);
            checkPhyDdlExceps = checkPhyDdlExcepsEmpty();
            jobInterrupted = CrossEngineValidator.isJobInterrupted(originEc);
        }
        if (!phyDdlExceps.isEmpty() || inRollbackState(runningState)) {
            // Interrupt all.
            phyDdlTasks.forEach(f -> {
                try {
                    f.cancel(true);
                } catch (Throwable ignore) {
                }
            });
            runningState.set(RunningState.ROLLBACK);
//            twoPhaseDdlReporter.collectStatsAndUpdateState(runningState);
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                StringUtil.join(",", phyDdlExceps.stream().map(
                    Throwable::getMessage).collect(Collectors.toList())).toString());
        } else if (jobInterrupted) {
            // Interrupted.
            runningState.set(RunningState.PAUSED);
        }
        return !jobInterrupted;
    }

    public int twoPhaseDdlPrepare(String schemaName, String logicalTable,
                                  ExecutionContext executionContext) throws InterruptedException, ExecutionException {
        return twoPhaseDdlEvolve(schemaName, logicalTable, SQL_PREPARE_TWO_PHASE_DDL,
            TWO_PHASE_DDL_PREPARE_TASK_NAME,
            executionContext, false);
    }

    public int twoPhaseDdlWait(String schemaName, String logicalTable, String taskName,
                               Set<String> expectedPhyDdlStates,
                               ExecutionContext executionContext) throws InterruptedException, ExecutionException {
        checkAllPhyDdlInState(logicalTable, expectedPhyDdlStates, executionContext);
        return twoPhaseDdlEvolve(schemaName, logicalTable, SQL_WAIT_TWO_PHASE_DDL, taskName,
            executionContext, true);
    }

    public int twoPhaseDdlCommit(String schemaName, String logicalTable,
                                 ExecutionContext executionContext) throws InterruptedException, ExecutionException {
        return twoPhaseDdlCommit(schemaName, logicalTable, SQL_COMMIT_TWO_PHASE_DDL,
            TWO_PHASE_DDL_COMMIT_TASK_NAME,
            executionContext);
    }

    public int twoPhaseDdlFinish(String schemaName, String logicalTable,
                                 ExecutionContext executionContext) throws InterruptedException, ExecutionException {
        ExecutionContext ec = executionContext.copy();
        AtomicInteger unfinishedCount = new AtomicInteger(sourcePhyTableNames.size());
        this.runningState.set(RunningState.FINISH);
        waitAllPhysicalDdlFinished(schemaName, logicalTable, executionContext);
        while (unfinishedCount.get() > 0) {
            sourcePhyTableNames.keySet().forEach(sourceGroupName -> {
                String sql =
                    String.format(SQL_FINISH_TWO_PHASE_DDL, schemaName,
                        buildTwoPhaseKeyFromLogicalTableNameAndGroupName(
                            logicalTable, sourceGroupName
                        ));
                List<Map<String, Object>> results =
                    queryGroupBypassConnPool(ec, jobId, TWO_PHASE_DDL_FINISH_TASK_NAME, schemaName,
                        logicalTable,
                        sourceGroupName,
                        sql);
                if (resultsToFinishSuccess(results)) {
                    unfinishedCount.getAndDecrement();
                }
            });
            Thread.sleep(DEFAULT_WAIT_TIME_MS);
        }
        return 1;
    }

    public void twoPhaseDdlLog(String schemaName, String logicalTable,
                               ExecutionContext executionContext) throws InterruptedException, ExecutionException {
        List<Map<String, Object>> results = DdlHelper.getServerConfigManager()
            .executeQuerySql(TwoPhaseDdlUtils.SQL_SHOW_PHYSICAL_DDL, schemaName, null);
//        result.addColumn("PHYSICAL_DB_NAME", DataTypes.StringType);
//        result.addColumn("PHYSICAL_TABLE_NAME", DataTypes.StringType);
//        result.addColumn("PHASE", DataTypes.StringType);
//        result.addColumn("STATE", DataTypes.StringType);
//        result.addColumn("PROCESS_ID", DataTypes.LongType);
//        result.addColumn("PROCESS_STATE", DataTypes.StringType);
//        result.addColumn("TIME", DataTypes.LongType);
//
//        result.addColumn("REACHED_PREPARED_MOMENT", DataTypes.StringType);
//        result.addColumn("REACHED_COMMIT_MOMENT", DataTypes.StringType);
//        result.addColumn("COMMIT_MOMENT", DataTypes.StringType);
//        result.addColumn("PREPARE_MOMENT", DataTypes.StringType);
//        result.addColumn("PREPARED_RUNNING_CONSUME_BLOCKS", DataTypes.LongType);
//        result.addColumn("PREPARED_RUNNING_CONSUME_TIME", DataTypes.LongType);
//        result.addColumn("COMMIT_CONSUME_BLOCKS", DataTypes.LongType);
//        result.addColumn("COMMIT_CONSUME_TIME", DataTypes.LongType);
//        result.addColumn("LOCK_TABLE_TIME", DataTypes.LongType);
        int totalPhyTableNum = results.size();
        long maxLockTableTime = 0L;
        long minLockTableTime = 1000_000_000_000L;
        int finished = 0;
        for (Map<String, Object> line : results) {
            long lockTableTime = (Long) line.get("LOCK_TABLE_TIME");
            if (lockTableTime >= maxLockTableTime) {
                maxLockTableTime = lockTableTime;
            } else if (lockTableTime <= minLockTableTime) {
                minLockTableTime = lockTableTime;
            }
            String phase = (String) line.get("PHASE");
            if (phase.equalsIgnoreCase("COMMIT")) {
                finished += 1;
            }
        }
        String logInfo = String.format(
            "<MultiPhaseDdl %d> schema: %s, table: %s, sql: %s, Two Phase ddl task finished! finished %d physcial table, max_lock_table_time is %d, min_lock_table_time is %d",
            jobId, schemaName, logicalTable, phyDdlStmt,
            finished,
            maxLockTableTime, minLockTableTime);
        LOG.info(logInfo);
        EventLogger.log(EventType.TWO_PHASE_DDL_INFO, logInfo);
        String showDdlResults = JSON.toJSONString(results, SerializerFeature.DisableCircularReferenceDetect);
        if (showDdlResults.length() > 1000_00L) {
            showDdlResults = DdlHelper.compress(showDdlResults);
        }
        EventLogger.log(EventType.TWO_PHASE_DDL_INFO, showDdlResults);
    }

    public int waitAllPhysicalDdlFinished(String schemaName, String logicalTable,
                                          ExecutionContext executionContext)
        throws InterruptedException, ExecutionException {
        int status = 1;
        ExecutionContext ec = executionContext.copy();
        if (sourcePhyTableNames == null || sourcePhyTableNames.isEmpty()) {
            return status;
        }
        Set<String> unfinishedSourceGroupNames = new HashSet<>(sourcePhyTableNames.keySet());
        while (!unfinishedSourceGroupNames.isEmpty()) {
            collectPhysicalDdlStats(schemaName, logicalTable, executionContext);
            List<String> unfinishedSourceGroupNameList = new ArrayList<>(unfinishedSourceGroupNames);
            for (String sourceGroupName : unfinishedSourceGroupNameList) {
                Map<String, String> processInfoMap = this.queryProcessInfoMap.get(sourceGroupName);
                if (checkProcessFinished(schemaName, sourceGroupName, processInfoMap)) {
                    unfinishedSourceGroupNames.remove(sourceGroupName);
                }
            }
            Thread.sleep(DEFAULT_WAIT_TIME_MS);
        }
        return status;
    }

    public Boolean checkIfProcessInfoRunning(String processInfo) {
        return !StringUtils.isEmpty(processInfo) && processInfo.contains(twoPhaseDdlManagerId.toString());
    }

    public Boolean checkProcessFinished(String schemaName, String logicalTable,
                                        Map<String, String> processInfoMap) {
        for (String phyTableName : processInfoMap.keySet()) {
            String processInfo = processInfoMap.get(phyTableName);
            // for jdbc: the thread would disappear soon.
            // for xprotocal: the thread may remain alive for other request.
            if (checkIfProcessInfoRunning(processInfo)) {
                return false;
            }
        }
        String logInfo =
            String.format("<MultiPhaseDdl %d> %s.%s ddl finished, the process info is %s", jobId, schemaName,
                logicalTable,
                processInfoMap);
        LOG.info(logInfo);
        return true;
    }

    public void collectPhysicalDdlStats(String schemaName, String logicalTable,
                                        ExecutionContext executionContext) {
        if (sourcePhyTableNames == null || sourcePhyTableNames.isEmpty()) {
            return;
        }
        ExecutionContext ec = executionContext.copy();
        sourcePhyTableNames.keySet().forEach(sourceGroupName -> {
            String sql =
                String.format(SQL_STATS_TWO_PHASE_DDL, schemaName, buildTwoPhaseKeyFromLogicalTableNameAndGroupName(
                    logicalTable, sourceGroupName
                ));
            List<Map<String, Object>> results =
                queryGroupBypassConnPool(ec, jobId, TWO_PHASE_DDL_STATS, schemaName,
                    logicalTable,
                    sourceGroupName,
                    sql);
            Map<String, Long> queryIdMapOnSourceGroup = resultsToQueryIdMap(results);
            Map<String, String> physicalDdlStateMapOnSourceGroup = resultsToPhysicalDdlStateMap(results);
            String phase = resultsToPhysicalDdlPhase(results);
            this.queryIdMap.putAll(queryIdMapOnSourceGroup);
            this.queryPhyDdlRunningStateMap.put(sourceGroupName, physicalDdlStateMapOnSourceGroup);
            this.queryPhyDdlPhaseMap.put(sourceGroupName, phase);
            List<Map<String, Object>> processInfoResults =
                queryGroupBypassConnPool(ec, jobId, TWO_PHASE_DDL_STATS, schemaName,
                    logicalTable,
                    sourceGroupName,
                    SQL_SHOW_PROCESS_LIST);
            Map<Pair<String, Long>, String> queryIdToProcessInfoMap =
                resultsToQueryIdToProcessInfoMap(processInfoResults);
            Map<String, String> physicalTableNameToProcessInfoMap = new HashMap<>();
            String phyDbName = buildPhysicalDbNameFromGroupName(sourceGroupName);
            for (String physicalTableName : queryIdMapOnSourceGroup.keySet()) {
                Long queryId = queryIdMapOnSourceGroup.get(physicalTableName);
                physicalTableNameToProcessInfoMap.put(physicalTableName,
                    queryIdToProcessInfoMap.getOrDefault(Pair.of(phyDbName, queryId), null));
            }
            this.queryProcessInfoMap.put(sourceGroupName, physicalTableNameToProcessInfoMap);
        });
    }

    public Boolean phyTableInCommitState(String groupName) {
        return this.queryPhyDdlPhaseMap.get(groupName).equalsIgnoreCase(COMMIT_STATE);
    }

    public Boolean phyTableInRollbackState(String groupName) {
        return this.queryPhyDdlRunningStateMap.get(groupName).values().stream()
            .anyMatch(o -> o.equalsIgnoreCase(WAIT_ROLLBACK_STATE));
    }

    public int twoPhaseDdlRollback(String schemaName, String logicalTable,
                                   ExecutionContext executionContext) throws InterruptedException, ExecutionException {

        long startTime = System.currentTimeMillis();
        int status = 1;
        this.runningState.set(RunningState.ROLLBACK);
        ExecutionContext ec = executionContext.copy();
        if (sourcePhyTableNames == null || sourcePhyTableNames.isEmpty()) {
            return status;
        }
        EventLogger.log(EventType.TWO_PHASE_DDL_INFO, "multiple phase ddl start rollbacking...");

        collectPhysicalDdlStats(schemaName, logicalTable, executionContext);
        sourcePhyTableNames.forEach((sourceGroupName, phyTableNames) -> {
            List<String> sqls = new ArrayList<>();
            phyTableNames.forEach(phyTableName -> {
                String fullPhyTableName =
                    buildPhyDbTableNameFromGroupNameAndPhyTableName(sourceGroupName, phyTableName);
                Long queryId = queryIdMap.get(fullPhyTableName);
                String processInfo = queryProcessInfoMap.get(sourceGroupName).get(fullPhyTableName);
                // if there is truely running ddl, kill query.
                if (queryId != null && queryId > 0 && checkIfProcessInfoRunning(processInfo)) {
                    String sql = String.format(SQL_KILL_QUERY, queryId);
                    sqls.add(sql);
                }
            });
            if (!sqls.isEmpty()) {
                String multipleKillSql = StringUtil.join(";", sqls).toString();
                updateGroupBypassConnPool(ec, jobId, TWO_PHASE_DDL_ROLLBACK_TASK_NAME, schemaName, logicalTable,
                    sourceGroupName, multipleKillSql);
            }
        });
        sourcePhyTableNames.forEach((sourceGroupName, phyTableNames) -> {
            String sql =
                String.format(SQL_ROLLBACK_TWO_PHASE_DDL, schemaName, buildTwoPhaseKeyFromLogicalTableNameAndGroupName(
                    logicalTable, sourceGroupName
                ));
            queryGroupBypassConnPool(ec, jobId, TWO_PHASE_DDL_ROLLBACK_TASK_NAME, schemaName,
                logicalTable,
                sourceGroupName,
                sql);
        });
        waitAllPhysicalDdlFinished(schemaName, logicalTable, executionContext);
        Set<String> residueSourceGroupNames = new HashSet<>(sourcePhyTableNames.keySet());
        while (!residueSourceGroupNames.isEmpty()) {
            String sourceGroupName = residueSourceGroupNames.iterator().next();
            String sql =
                String.format(SQL_FINISH_TWO_PHASE_DDL, schemaName,
                    buildTwoPhaseKeyFromLogicalTableNameAndGroupName(
                        logicalTable, sourceGroupName
                    ));
            List<Map<String, Object>> results =
                queryGroupBypassConnPool(ec, jobId, TWO_PHASE_DDL_ROLLBACK_TASK_NAME, schemaName,
                    logicalTable,
                    sourceGroupName,
                    sql);
            if (resultsToFinishSuccess(results)) {
                LOG.info(
                    String.format("<MultiPhaseDdl %d> execute %s success on group %s", jobId, sql, sourceGroupName));
                residueSourceGroupNames.remove(sourceGroupName);
            }
            Thread.sleep(FINISH_DDL_WAIT_TIME);
        }
        long finishTime = System.currentTimeMillis();
        long duration = finishTime - startTime;
        String logInfo =
            String.format("<MultiPhaseDdl %d> multiple phase ddl finished rollbacking, which cost %d ms", jobId,
                duration);
        EventLogger.log(EventType.TWO_PHASE_DDL_INFO, logInfo);
        LOG.info(logInfo);
        List<String> rollbackCause =
            phyDdlExceps.stream().filter(o -> !StringUtil.isNullOrEmpty(o.getMessage())).map(o -> o.getMessage())
                .collect(
                    Collectors.toList());
        String rollbackCauseString = StringUtil.join(", ", rollbackCause).toString();
        EventLogger.log(EventType.TWO_PHASE_DDL_INFO,
            String.format("<MultiPhaseDdl %d> multiple phase ddl rollback because %s ", jobId, rollbackCauseString));
        return status;
    }

    public Boolean checkPhyDdlExcepsEmpty() {
        return phyDdlExceps.isEmpty();
//        if (!phyDdlExceps.isEmpty()) {
        // Interrupt all.
//            phyDdlTasks.forEach(f -> {
//                try {
//                    f.cancel(true);
//                } catch (Throwable ignore) {
//                }
//            });
    }

    public void initPhyTableDdl(String schemaName, String logicalTable, String sourceGroupName,
                                Set<String> phyTableNames, ExecutionContext executionContext) {
        ExecutionContext ec = executionContext.copy();
        // there may contain "'" in phy table names.
        List<String> phyTableNamesList =
            phyTableNames.stream().map(o -> String.format("'%s'", o.toLowerCase().replace("'", "\\'"))).collect(
                Collectors.toList());
        String phyTableNameStr = TStringUtil.join(phyTableNamesList, ",");
        String sql =
            String.format(SQL_INIT_TWO_PHASE_DDL, schemaName, buildTwoPhaseKeyFromLogicalTableNameAndGroupName(
                    logicalTable, sourceGroupName
                ),
                phyTableNameStr);
        List<List<Object>> results =
            queryGroup(ec, jobId, TWO_PHASE_DDL_INIT_TASK_NAME, schemaName, logicalTable, sourceGroupName, sql);

        if (!TwoPhaseDdlData.initialPhyDdlSuccess(results)) {
            throw GeneralUtil.nestedException(
                String.format("failed to initialize two phase ddl on group(%s): %s , Caused by unknown reason",
                    sourceGroupName, formatString(sql)));
        }
    }

    public int twoPhaseDdlEvolve(String schemaName, String logicalTable,
                                 String sqlStmt, String taskName,
                                 ExecutionContext executionContext,
                                 Boolean checkDdlInterrupted) throws InterruptedException, ExecutionException {
        int status = 1;
        ExecutionContext ec = executionContext.copy();
        List<Future> futures = new ArrayList<>();
        if (sourcePhyTableNames == null || sourcePhyTableNames.isEmpty()) {
            return status;
        }

        Map<FutureTask, String> futureTaskMap = new HashMap<>();
        String hint = String.format(TWO_PHASE_PHYSICAL_DDL_HINT_TEMPLATE, twoPhaseDdlManagerId);
        sourcePhyTableNames.forEach((sourceGroupName, phyTableNames) -> {
            String sql = hint + String.format(sqlStmt, schemaName, buildTwoPhaseKeyFromLogicalTableNameAndGroupName(
                logicalTable, sourceGroupName
            ));
            FutureTask<List<List<Object>>> task = new FutureTask<>(
                () -> queryGroup(ec, jobId, taskName, schemaName,
                    logicalTable,
                    sourceGroupName,
                    sql));
            futures.add(task);
            futureTaskMap.put(task, sourceGroupName);
            TwoPhaseDdlThreadPool.getInstance()
                .executeWithContext(task, PriorityFIFOTask.TaskPriority.HIGH_PRIORITY_TASK);
        });

        while (!futures.stream().allMatch(o -> o.isDone())) {
            Boolean phyDdlExcepsEmpty = checkPhyDdlExcepsEmpty();
            Boolean jobInterrupted = CrossEngineValidator.isJobInterrupted(executionContext);
            if (!phyDdlExcepsEmpty || (checkDdlInterrupted && jobInterrupted)) {
                status = 0;
                break;
            } else {
                Thread.sleep(TwoPhaseDdlManager.DEFAULT_WAIT_TIME_MS);
            }
        }

        if (status == 0 && !futures.stream().allMatch(o -> o.isDone())) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                StringUtil.join(",", phyDdlExceps.stream().map(
                    Throwable::getMessage).collect(Collectors.toList())).toString());
        }

        for (Future future : futures) {
            List<List<Object>> results = (List<List<Object>>) future.get();
            LOG.info(String.format("<MultiPhaseDdl %d> [%s] %s FutureTask on group [%s] get result {%s}",
                jobId, Thread.currentThread().getName(), taskName, futureTaskMap.get(future),
                TwoPhaseDdlUtils.resultToString(results)));
            if (!TwoPhaseDdlData.evolvePhyDdlSuccess(results)) {
                status = 0;
            }
        }

        if (status == 0) {
            Boolean phyDdlExcepsEmpty = checkPhyDdlExcepsEmpty();
            if (!phyDdlExcepsEmpty) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    StringUtil.join(",", phyDdlExceps.stream().map(
                        Throwable::getMessage).collect(Collectors.toList())).toString());
            }
        }
        return status;
    }

    public int twoPhaseDdlCommit(String schemaName, String logicalTable,
                                 String sqlStmt, String taskName,
                                 ExecutionContext executionContext) throws InterruptedException, ExecutionException {
        // TODO:(2pc-ddl): commit
        // Firstly try best to commit for 3 times.
        // Then rollback which count not commit failed for 3 times.
        int status = 1;
        int totalRetryTimes = 3;
        int retryTime = 0;
        ExecutionContext ec = executionContext.copy();
        if (sourcePhyTableNames == null || sourcePhyTableNames.isEmpty()) {
            return status;
        }
        long startTime = System.currentTimeMillis();
        EventLogger.log(EventType.TWO_PHASE_DDL_INFO, "multiple phase ddl start committing...");

        Set<String> failedGroups = new HashSet<>(sourcePhyTableNames.keySet());
        while (retryTime < totalRetryTimes) {
            List<Future> futures = new ArrayList<>();
            status = 1;
            collectPhysicalDdlStats(schemaName, logicalTable, executionContext);
            Map<FutureTask, String> futureTaskMap = new HashMap<>();
            sourcePhyTableNames.forEach((sourceGroupName, phyTableNames) -> {
                if (!failedGroups.contains(sourceGroupName)) {
                    return;
                }
                String sql = String.format(sqlStmt, schemaName, buildTwoPhaseKeyFromLogicalTableNameAndGroupName(
                    logicalTable, sourceGroupName
                ));
                FutureTask<List<List<Object>>> task = new FutureTask<>(
                    () -> queryGroup(ec, jobId, taskName, schemaName,
                        logicalTable,
                        sourceGroupName,
                        sql));
                futures.add(task);
                futureTaskMap.put(task, sourceGroupName);
                TwoPhaseDdlThreadPool.getInstance()
                    .executeWithContext(task, PriorityFIFOTask.TaskPriority.HIGH_PRIORITY_TASK);
            });

            while (!futures.stream().allMatch(o -> o.isDone())) {
                Thread.sleep(TwoPhaseDdlManager.DEFAULT_WAIT_TIME_MS);
                // We will still continue to guaranteen all is committed.
            }

            collectPhysicalDdlStats(schemaName, logicalTable, executionContext);

            for (Future future : futures) {
                List<List<Object>> results = (List<List<Object>>) future.get();
                LOG.info(String.format("<MultiPhaseDdl %d> [%s] %s FutureTask on group [%s] get result %s",
                    jobId, Thread.currentThread().getName(), taskName, futureTaskMap.get(future),
                    TwoPhaseDdlUtils.resultToString(results)));
                String group = futureTaskMap.get(future);
                if (!TwoPhaseDdlData.evolvePhyDdlSuccess(results) || !phyTableInCommitState(group)) {
                    status = 0;
                    failedGroups.add(group);
                } else {
                    failedGroups.remove(group);
                }
            }

            LOG.info(String.format("<MultiPhaseDdl %d> commit try time %d, get status %d",
                jobId, retryTime, status));
            retryTime += 1;
            if (status == 1) {
                break;
            }
        }
        retryTime = 0;
        while (status == 0 && retryTime < totalRetryTimes) {
            status = 1;
            collectPhysicalDdlStats(schemaName, logicalTable, executionContext);
            List<Future> futures = new ArrayList<>();
            Map<FutureTask, String> futureTaskMap = new HashMap<>();
            sourcePhyTableNames.forEach((sourceGroupName, phyTableNames) -> {
                if (!failedGroups.contains(sourceGroupName)) {
                    return;
                }
                String sql = String.format(SQL_ROLLBACK_TWO_PHASE_DDL, schemaName,
                    buildTwoPhaseKeyFromLogicalTableNameAndGroupName(
                        logicalTable, sourceGroupName
                    ));
                FutureTask<List<List<Object>>> task = new FutureTask<>(
                    () -> queryGroup(ec, jobId, taskName, schemaName,
                        logicalTable,
                        sourceGroupName,
                        sql));
                futures.add(task);
                futureTaskMap.put(task, sourceGroupName);
                TwoPhaseDdlThreadPool.getInstance()
                    .executeWithContext(task, PriorityFIFOTask.TaskPriority.HIGH_PRIORITY_TASK);
            });

            while (!futures.stream().allMatch(o -> o.isDone())) {
                Thread.sleep(TwoPhaseDdlManager.DEFAULT_WAIT_TIME_MS);
                status = 0;
                // We will still continue to guaranteen all is committed.
            }

            collectPhysicalDdlStats(schemaName, logicalTable, executionContext);
            for (Future future : futures) {
                List<List<Object>> results = (List<List<Object>>) future.get();
                String group = futureTaskMap.get(future);
                LOG.info(String.format("<MultiPhaseDdl %d> [%s] %s FutureTask on group [%s] get result %s",
                    jobId, Thread.currentThread().getName(), taskName, group,
                    TwoPhaseDdlUtils.resultToString(results)));
                if (!TwoPhaseDdlData.evolvePhyDdlSuccess(results) || !phyTableInRollbackState(group)) {
                    status = 0;
                    failedGroups.add(group);
                } else {
                    failedGroups.remove(group);
                }
            }
            retryTime += 1;
        }
        long finishTime = System.currentTimeMillis();
        long duration = finishTime - startTime;
        EventLogger.log(EventType.TWO_PHASE_DDL_INFO,
            String.format("multiple phase ddl finished committing, which cost %d ms", duration));
        return status;
    }

    public String generateAlterTableStmt(String phyDdlStmt, String phyTableName) {
        String drdsHint = String.format(TWO_PHASE_PHYSICAL_DDL_HINT_TEMPLATE, twoPhaseDdlManagerId);
        SQLAlterTableStatement alterTable = (SQLAlterTableStatement) FastsqlUtils.parseSql(phyDdlStmt).get(0);
        alterTable.setTableSource(new SQLExprTableSource(new SQLIdentifierExpr(
            SqlIdentifier.surroundWithBacktick(phyTableName))));
        String phyDdl = drdsHint + alterTable.toString();
        return phyDdl;
    }

    public void emitPhyTableDdl(String logicalTable, String taskName, String groupName, String phyTableName,
                                String phyDdlStmt,
                                ExecutionContext ec,
                                List<Exception> exceps,
                                AtomicInteger count) {
        // this is only used for logging, we still use prepare statement for executing sql.
        String phyDdl = generateAlterTableStmt(phyDdlStmt, phyTableName);
        String drdsHint = String.format(TWO_PHASE_PHYSICAL_DDL_HINT_TEMPLATE, twoPhaseDdlManagerId);
        try {
            if (inRunningState(runningState)) {
                LOG.info(String.format(
                    "<MultiPhaseDdl %d> [%s] %s execute physical ddl %s on group %s EMIT, logical table %s",
                    jobId, Thread.currentThread().getName(), taskName, formatString(phyDdl), groupName, logicalTable));
                lastEmitPhyTableNameMap.put(groupName, phyTableName);
                count.decrementAndGet();
                phyTableEmitted.get(groupName).add(phyTableName);
                executePhyDdlBypassConnPool(ec, jobId, schemaName, groupName, phyDdl, drdsHint, phyTableName);
                LOG.info(String.format(
                    "<MultiPhaseDdl %d> [%s] %s execute physical ddl %s on group %s END, logical table %s",
                    jobId, Thread.currentThread().getName(), taskName, formatString(phyDdl), groupName, logicalTable));
            } else {
                LOG.info(
                    String.format(
                        "<MultiPhaseDdl %d> [%s] %s skip execute physical ddl %s on group %s END, logical table %s",
                        jobId, Thread.currentThread().getName(), taskName, formatString(phyDdl), groupName,
                        logicalTable));
            }
        } catch (RuntimeException exception) {
            LOG.info(String.format(
                "<MultiPhaseDdl %d> [%s] %s skip execute physical ddl %s on group %s FAILED, logical table %s, return exception %s",
                jobId, Thread.currentThread().getName(), taskName, formatString(phyDdl), groupName, logicalTable,
                formatString(exception.getMessage())));
            exceps.add(exception);
        }
    }

    public Boolean inRunningState(AtomicReference<RunningState> runningState) {
        Set<RunningState> continueRunningPhase = new HashSet<>(
            Arrays.asList(RunningState.INIT, RunningState.PREPARE, RunningState.COMMIT)
        );
        return continueRunningPhase.contains(runningState.get());
    }

    public Boolean inRollbackState(AtomicReference<RunningState> runningState) {
        Set<RunningState> continueRunningPhase = new HashSet<>(
            Arrays.asList(RunningState.ROLLBACK)
        );
        return continueRunningPhase.contains(runningState.get());
    }

    public Boolean checkAllPhyDdlEmited(String logicalTableName, ExecutionContext executionContext) {
        collectPhysicalDdlStats(schemaName, logicalTableName, executionContext);
        Boolean allPhysicalTableEmitted = true;
        int emittedNum = 0;
        int remainNum = 0;
        for (String sourceGroupName : sourcePhyTableNames.keySet()) {
            Map<String, String> queryProcessInfoOnGroup = queryProcessInfoMap.get(sourceGroupName);
            Set<String> phyTableNames = sourcePhyTableNames.get(sourceGroupName);
            Set<String> emittedPhyTableNames = new HashSet<>();
            for (String phyTableName : phyTableNames) {
                String fullPhyTableName =
                    buildPhyDbTableNameFromGroupNameAndPhyTableName(sourceGroupName, phyTableName);
                String processInfo = queryProcessInfoOnGroup.get(fullPhyTableName);
                if (checkIfProcessInfoRunning(processInfo)) {
                    emittedPhyTableNames.add(phyTableName);
                    emittedNum++;
                } else {
                    allPhysicalTableEmitted = false;
                    remainNum++;
                }
            }
            phyTableEmitted.put(sourceGroupName, emittedPhyTableNames);
        }
        String logInfo = String.format(
            "<MultiPhaseDdl %d> check all the physical ddl emitted for %s, %d emitted, %d remain, emitted physical table name: %s",
            jobId, logicalTableName, emittedNum, remainNum, phyTableEmitted);
        LOG.info(logInfo);
        return allPhysicalTableEmitted;
    }

    public Boolean checkAllPhyDdlInState(String logicalTableName, Set<String> expectedStates,
                                         ExecutionContext executionContext) {
        collectPhysicalDdlStats(schemaName, logicalTableName, executionContext);
        Boolean allPhysicalTableInState = true;
        for (String sourceGroupName : sourcePhyTableNames.keySet()) {
            Map<String, String> queryProcessInfoOnGroup = queryProcessInfoMap.get(sourceGroupName);
            Map<String, String> phyDdlStateOnGroup = queryPhyDdlRunningStateMap.get(sourceGroupName);
            Set<String> phyTableNames = sourcePhyTableNames.get(sourceGroupName);
            for (String phyTableName : phyTableNames) {
                String fullPhyTableName =
                    buildPhyDbTableNameFromGroupNameAndPhyTableName(sourceGroupName, phyTableName);
                if (queryProcessInfoOnGroup.get(fullPhyTableName) == null || !expectedStates.contains(
                    phyDdlStateOnGroup.get(fullPhyTableName))) {
                    allPhysicalTableInState = false;
                }
            }
        }
        return allPhysicalTableInState;
    }

    public Boolean isPhysicalRunning(String phyTableName) {
        //TODO(2pc-ddl) IMPROVE: check if is physical running.
        //if allow retry, then we must deal with count problem on DN....
        //which require some check...
        return true;
    }

    private QueryConcurrencyPolicy getConcurrencyPolicy(ExecutionContext executionContext) {
        boolean mergeConcurrent = executionContext.getParamManager().getBoolean(ConnectionParams.MERGE_CONCURRENT);

        boolean mergeDdlConcurrent =
            executionContext.getParamManager().getBoolean(ConnectionParams.MERGE_DDL_CONCURRENT);

        boolean sequential =
            executionContext.getParamManager().getBoolean(ConnectionParams.SEQUENTIAL_CONCURRENT_POLICY);

        if (mergeConcurrent && mergeDdlConcurrent) {
            return QueryConcurrencyPolicy.CONCURRENT;
        } else if (mergeConcurrent) {
            return QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK;
        } else if (sequential) {
            return QueryConcurrencyPolicy.SEQUENTIAL;
        }

        return QueryConcurrencyPolicy.INSTANCE_CONCURRENT;
    }
}
