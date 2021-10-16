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
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.common.constants.CpuStatAttribute;
import com.alibaba.polardbx.common.constants.CpuStatAttribute.CpuStatAttr;
import com.alibaba.polardbx.common.model.SqlType;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.LogicalShowProfileHandler;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.profiler.cpu.CpuStat;
import com.alibaba.polardbx.optimizer.core.profiler.cpu.CpuStatItem;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.memory.QueryMemoryPool;
import com.alibaba.polardbx.optimizer.parse.SqlTypeUtils;
import com.alibaba.polardbx.optimizer.statis.MemoryStatisticsGroup;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.alibaba.polardbx.statistics.RuntimeStatistics.Metrics;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.sql.SqlShowProfile;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;
import org.apache.calcite.util.trace.RuntimeStatisticsSketchExt;
import org.apache.commons.lang3.StringUtils;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Stack;

/**
 * @author chenghui.lch
 */
public class ShowProfileSyncAction implements ISyncAction {

    protected static final MemoryComparator MEMORY_COMP = new MemoryComparator();
    protected static final CpuComparator CPU_COMP = new CpuComparator();
    protected static final DecimalFormat NUM_FORMAT = new DecimalFormat("#,###");
    protected static Set<SqlType> profileIgnoreSqlTypeSet = new HashSet<>();

    static {
        profileIgnoreSqlTypeSet.add(SqlType.SHOW);
        profileIgnoreSqlTypeSet.add(SqlType.SHOW_SEQUENCES);
        profileIgnoreSqlTypeSet.add(SqlType.SHOW_CHARSET);
        profileIgnoreSqlTypeSet.add(SqlType.SHOW_INSTANCE_TYPE);
        profileIgnoreSqlTypeSet.add(SqlType.TDDL_SHOW);
    }

    protected String queryId;
    protected List<String> types;

    public ShowProfileSyncAction() {
    }

    public ShowProfileSyncAction(List<String> types, String queryId) {
        this.queryId = queryId;
        this.types = types;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public List<String> getTypes() {
        return types;
    }

    public void setTypes(List<String> types) {
        this.types = types;
    }

    protected static class MemoryPoolItem {

        public Long queryId;
        public MemoryPool stmtPoolStat;
    }

    protected static class CpuSketchItem {

        public long queryId;
        public String traceId;
        public String dbName;
        public long logTc = 0L;
        public long phySqlTc = 0L;
        public long phyRsTc = 0L;
        public String sql = null;
    }

    protected static class RelNodeItem {

        public int relNodeId;
        public String relNodeName;
        public long timeCost = -1L;
        public long rowCount = -1L;
        public long parallelism = 0L;
    }

    protected static class MemoryComparator implements Comparator<MemoryPoolItem> {

        @Override
        public int compare(MemoryPoolItem o1, MemoryPoolItem o2) {
            MemoryPool o1PoolStat = o1.stmtPoolStat;
            MemoryPool o2PoolStat = o2.stmtPoolStat;
            if (o1PoolStat.getMemoryUsageStat() < o2PoolStat.getMemoryUsageStat()) {
                return 1;
            } else if (o1PoolStat.getMemoryUsageStat() > o2PoolStat.getMemoryUsageStat()) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    protected static class CpuComparator implements Comparator<CpuSketchItem> {

        @Override
        public int compare(CpuSketchItem o1, CpuSketchItem o2) {
            if (o1.logTc < o2.logTc) {
                return 1;
            } else if (o1.logTc > o2.logTc) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    protected static class RelItemVisitor extends RelVisitor {

        protected List<RelNodeItem> relNodeItemList = new ArrayList<>();
        protected Stack<RelNode> relNodeStack = new Stack<>();
        protected RuntimeStatistics runtimeStatistics = null;
        protected Map<RelNode, RuntimeStatisticsSketch> relStatMaps = null;

        public RelItemVisitor(RuntimeStatistics runtimeStatistics) {
            this.runtimeStatistics = runtimeStatistics;
            this.relStatMaps = this.runtimeStatistics.toSketchExt();
        }

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {

            buildRelNodeItem(node);
            relNodeStack.push(node);
            if (!(node instanceof LogicalView || node instanceof LogicalInsert)) {
                List<RelNode> inputs = node.getInputs();
                for (int i = 0; i < inputs.size(); i++) {
                    visit(inputs.get(i), i, node);
                }
            }
            relNodeStack.pop();
        }

        @Override
        public RelNode go(RelNode p) {
            this.replaceRoot(p);
            visit(p, 0, null);
            return p;
        }

        protected void buildRelNodeItem(RelNode p) {
            RelNodeItem relNodeItem = new RelNodeItem();
            relNodeItem.relNodeId = p.getId();
            relNodeItem.relNodeName = buildRelNameWithIndents(relNodeStack.size(), p);

            boolean isLvOrLm = false;
            if (p instanceof LogicalView || p instanceof LogicalInsert) {
                isLvOrLm = true;
            } else if (runtimeStatistics.isFromAllAtOnePhyTable()) {
                // When the final plan is only a PhyTableOperation,
                // We should treat it as LogicalView as well.
                isLvOrLm = true;
            }

            RuntimeStatisticsSketchExt statSketch = (RuntimeStatisticsSketchExt) relStatMaps.get(p);
            if (statSketch != null) {
                long timeCostSumOfInputs = 0;
                if (!isLvOrLm) {
                    List<RelNode> inputList = p.getInputs();
                    for (int i = 0; i < inputList.size(); i++) {
                        RuntimeStatisticsSketchExt statSketchOfInput =
                            (RuntimeStatisticsSketchExt) relStatMaps.get(inputList.get(i));
                        if (statSketchOfInput != null) {
                            timeCostSumOfInputs += statSketchOfInput.getStartupDurationNano()
                                + statSketchOfInput.getDurationNano()
                                + statSketchOfInput.getCloseDurationNano()
                                + statSketchOfInput.getChildrenAsyncTaskDuration()
                                + statSketchOfInput.getSelfAsyncTaskDuration();
                        }
                    }
                    relNodeItem.parallelism = statSketch.getInstances();
                }

                if (!statSketch.hasInputOperator()) {
                    relNodeItem.timeCost = statSketch.getStartupDurationNano() + statSketch.getDurationNano()
                        + statSketch.getCloseDurationNano() + statSketch.getSelfAsyncTaskDuration()
                        + statSketch.getChildrenAsyncTaskDuration();
                } else {
                    relNodeItem.timeCost = statSketch.getStartupDurationNano() + statSketch.getDurationNano()
                        + statSketch.getCloseDurationNano() + statSketch.getSelfAsyncTaskDuration()
                        + statSketch.getChildrenAsyncTaskDuration() - timeCostSumOfInputs;
                }

                relNodeItem.rowCount = statSketch.getRowCount();

                relNodeItemList.add(relNodeItem);
            }

            if (isLvOrLm && statSketch != null) {

                RelNodeItem newItem = null;
                newItem = new RelNodeItem();
                newItem.relNodeId = 0;
                newItem.relNodeName = buildStageTagsWithIndents(relNodeStack.size() + 1,
                    CpuStatAttribute.CREATE_CONN_TC_SUM);
                newItem.timeCost = statSketch.getCreateConnDurationNanoSum();
                if (newItem.timeCost > 0) {
                    relNodeItemList.add(newItem);
                }

                newItem = new RelNodeItem();
                newItem.relNodeId = 0;
                newItem.relNodeName = buildStageTagsWithIndents(relNodeStack.size() + 1,
                    CpuStatAttribute.WAIT_CONN_TC_SUM);
                newItem.timeCost = statSketch.getWaitConnDurationNanoSum();
                if (newItem.timeCost > 0) {
                    relNodeItemList.add(newItem);
                }

                newItem = new RelNodeItem();
                newItem.relNodeId = 0;
                newItem.relNodeName = buildStageTagsWithIndents(relNodeStack.size() + 1,
                    CpuStatAttribute.INIT_CONN_TC_SUM);
                newItem.timeCost = statSketch.getInitConnDurationNanoSum();
                if (newItem.timeCost > 0) {
                    relNodeItemList.add(newItem);
                }

                newItem = new RelNodeItem();
                newItem.relNodeId = 0;
                newItem.relNodeName = buildStageTagsWithIndents(relNodeStack.size() + 1,
                    CpuStatAttribute.PREPARE_STMT_TS_SUM);
                newItem.timeCost = statSketch.getCreateAndInitJdbcStmtDurationNanoSum();
                if (newItem.timeCost > 0) {
                    relNodeItemList.add(newItem);
                }

                newItem = new RelNodeItem();
                newItem.relNodeId = 0;
                newItem.relNodeName = buildStageTagsWithIndents(relNodeStack.size() + 1,
                    CpuStatAttribute.PHY_SQL_EXEC_TS_SUM);
                newItem.timeCost = statSketch.getExecJdbcStmtDurationNanoSum();
                if (newItem.timeCost > 0) {
                    relNodeItemList.add(newItem);
                }

                newItem = new RelNodeItem();
                newItem.relNodeId = 0;
                newItem.relNodeName = buildStageTagsWithIndents(relNodeStack.size() + 1,
                    CpuStatAttribute.PHY_SQL_FETCH_RS_TS_SUM);
                newItem.timeCost = statSketch.getFetchJdbcResultSetDurationNanoSum();
                if (newItem.timeCost > 0) {
                    relNodeItemList.add(newItem);
                }

                newItem = new RelNodeItem();
                newItem.relNodeId = 0;
                newItem.relNodeName = buildStageTagsWithIndents(relNodeStack.size() + 1,
                    CpuStatAttribute.PHY_SQL_CLOSE_RS_TS_SUM);
                newItem.timeCost = statSketch.getCloseJdbcResultSetDurationNanoSum();
                if (newItem.timeCost > 0) {
                    relNodeItemList.add(newItem);
                }

            }

        }

        protected String buildStageTagsWithIndents(int stackDeep, String stageTags) {
            StringBuilder stageTagsWithIndents = new StringBuilder("");
            for (int i = 0; i < stackDeep + 1; i++) {
                stageTagsWithIndents.append(LogicalShowProfileHandler.PROFILE_INDENTS_UNIT);
            }
            return stageTagsWithIndents.append(stageTags).toString();
        }

        protected String buildRelNameWithIndents(int stackDeep, RelNode rel) {
            StringBuilder relNameWithIndents = new StringBuilder("");
            for (int i = 0; i < stackDeep + 1; i++) {
                relNameWithIndents.append(LogicalShowProfileHandler.PROFILE_INDENTS_UNIT);
            }
            String relName = rel.getClass().getSimpleName();
            if (rel instanceof HashAgg) {
                if (((HashAgg) rel).isPartial()) {
                    relName = "PartialHashAgg";
                }
            }
            return relNameWithIndents.append(relName).toString();
        }

        public List<RelNodeItem> getRelNodeItemList() {
            return relNodeItemList;
        }
    }

    protected static class AllocationStat {

        public String allocId;
        public long usedSize;
        public long usedPeak;

        public AllocationStat() {
        }

        public AllocationStat(String allocId, long usedSize, long usedPeak) {
            this.allocId = allocId;
            this.usedSize = usedSize;
            this.usedPeak = usedPeak;
        }
    }

    protected static class AllocationComparator implements Comparator<AllocationStat> {

        @Override
        public int compare(AllocationStat ma1, AllocationStat ma2) {
            long t1 = ma1.usedSize;
            long t2 = ma2.usedSize;
            if (t1 > t2) {
                return 1;
            } else if (t1 < t2) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    protected ResultCursor syncForMemory() {
        String memUsageStr = "";
        String memUsageMaxStr = "";
        String memLimitStr = "";

        if (queryId == null) {

            ArrayResultCursor result = new ArrayResultCursor(LogicalShowProfileHandler.PROFILE_TABLE_NAME);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_POOL_NAME, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_CONN_ID, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_NODE_HOST, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_USED, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_PEAK, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_LIMIT, DataTypes.StringType);
            result.initMeta();

            String nodeHost = CobarServer.getInstance().getServerAddress();

            // Add Global Pool
            MemoryPool globalPool = MemoryManager.getInstance().getGlobalMemoryPool();
            memUsageStr = NUM_FORMAT.format(globalPool.getMemoryUsage());
            memUsageMaxStr = NUM_FORMAT.format(globalPool.getMaxMemoryUsage());
            memLimitStr = NUM_FORMAT.format(globalPool.getMaxLimit());
            result.addRow(new Object[] {
                globalPool.getFullName(), LogicalShowProfileHandler.PROFILE_NO_VALUE,
                nodeHost, memUsageStr, memUsageMaxStr, memLimitStr});

            // Add Cached Pool
            MemoryPool cachePool = MemoryManager.getInstance().getCacheMemoryPool();
            memUsageStr = NUM_FORMAT.format(cachePool.getMemoryUsage());
            memUsageMaxStr = NUM_FORMAT.format(cachePool.getMaxMemoryUsage());
            memLimitStr = NUM_FORMAT.format(cachePool.getMaxLimit());
            result.addRow(new Object[] {
                cachePool.getFullName(), LogicalShowProfileHandler.PROFILE_NO_VALUE,
                nodeHost, memUsageStr, memUsageMaxStr, memLimitStr});

            // Add Sorted StmtPool of General Pool
            PriorityQueue<MemoryPoolItem> allSortedStmtPoolList = new PriorityQueue<>(MEMORY_COMP);
            for (NIOProcessor p : CobarServer.getInstance().getProcessors()) {
                for (FrontendConnection fc : p.getFrontends().values()) {
                    if (fc != null && fc instanceof ServerConnection) {
                        ServerConnection sc = (ServerConnection) fc;
                        if (sc.isStatementExecuting().get()) {
                            ExecutionContext ec = sc.getTddlConnection().getExecutionContext();
                            if (ec.getRuntimeStatistics() == null) {
                                continue;
                            }
                            MemoryPoolItem stmtPoolItem = new MemoryPoolItem();
                            stmtPoolItem.queryId = sc.getId();
                            stmtPoolItem.stmtPoolStat = ec.getRuntimeStatistics().getMemoryPool();
                            allSortedStmtPoolList.add(stmtPoolItem);
                        }
                    }
                }
            }

            while (!allSortedStmtPoolList.isEmpty()) {
                MemoryPoolItem stmtPoolItem = allSortedStmtPoolList.poll();
                MemoryPool stmtPoolStat = stmtPoolItem.stmtPoolStat;
                Long memUsage = stmtPoolStat.getMemoryUsageStat();
                Long memUsageMax = stmtPoolStat.getMaxMemoryUsage();
                Long memLimit = stmtPoolStat.getMaxLimit();
                memUsageStr = NUM_FORMAT.format(memUsage);
                memUsageMaxStr = NUM_FORMAT.format(memUsageMax);
                memLimitStr = NUM_FORMAT.format(memLimit);
                result.addRow(new Object[] {
                    stmtPoolStat.getFullName(), String.valueOf(stmtPoolItem.queryId), nodeHost,
                    memUsageStr, memUsageMaxStr, memLimitStr});
            }

            // Add Sorted CachePool
            PriorityQueue<MemoryPoolItem> allSortedCacheList = new PriorityQueue<>(MEMORY_COMP);
            for (Map.Entry<String, MemoryPool> dbMemStatItem : cachePool.getChildren().entrySet()) {
                MemoryPool dbMemoryStat = dbMemStatItem.getValue();
                for (Map.Entry<String, MemoryPool> planMemStatItem : dbMemoryStat.getChildren().entrySet()) {
                    MemoryPool planMemoryStat = planMemStatItem.getValue();
                    MemoryPoolItem stmtPoolItem = new MemoryPoolItem();
                    stmtPoolItem.stmtPoolStat = planMemoryStat;
                    stmtPoolItem.queryId = null;
                    allSortedCacheList.add(stmtPoolItem);
                }
            }

            while (!allSortedCacheList.isEmpty()) {
                MemoryPoolItem planPoolItem = allSortedCacheList.poll();
                MemoryPool planPoolStat = planPoolItem.stmtPoolStat;
                Long memUsage = planPoolStat.getMemoryUsageStat();
                Long memUsageMax = planPoolStat.getMaxMemoryUsage();
                Long memLimit = planPoolStat.getMaxLimit();
                memUsageStr = NUM_FORMAT.format(memUsage);
                memUsageMaxStr = NUM_FORMAT.format(memUsageMax);
                memLimitStr = NUM_FORMAT.format(memLimit);
                result.addRow(new Object[] {
                    planPoolStat.getFullName(), LogicalShowProfileHandler.PROFILE_NO_VALUE,
                    nodeHost, memUsageStr, memUsageMaxStr, memLimitStr});
            }

            // Add Task
            //TODO
            //FIXME
            PriorityQueue<MemoryPoolItem> allSortedTaskList = new PriorityQueue<>(MEMORY_COMP);
            Collection<MemoryPool> schemaMemoryPools = globalPool.getChildren().values();
            for (MemoryPool schemaPool : schemaMemoryPools) {
                Collection<MemoryPool> queryPools = schemaPool.getChildren().values();
                for (MemoryPool queryPool : queryPools) {
                    Collection<MemoryPool> taskPools = queryPool.getChildren().values();
                    for (MemoryPool taskPool : taskPools) {
                        if (taskPool.getMemoryType() == MemoryType.TASK) {
                            MemoryPoolItem stmtPoolItem = new MemoryPoolItem();
                            stmtPoolItem.stmtPoolStat = taskPool;
                            stmtPoolItem.queryId = null;
                            allSortedTaskList.add(stmtPoolItem);
                        }
                    }
                }
            }
            while (!allSortedTaskList.isEmpty()) {
                MemoryPoolItem planPoolItem = allSortedTaskList.poll();
                MemoryPool planPoolStat = planPoolItem.stmtPoolStat;
                Long memUsage = planPoolStat.getMemoryUsageStat();
                Long memUsageMax = planPoolStat.getMaxMemoryUsage();
                Long memLimit = planPoolStat.getMaxLimit();
                memUsageStr = NUM_FORMAT.format(memUsage);
                memUsageMaxStr = NUM_FORMAT.format(memUsageMax);
                memLimitStr = NUM_FORMAT.format(memLimit);
                result.addRow(new Object[] {
                    planPoolStat.getFullName(), LogicalShowProfileHandler.PROFILE_NO_VALUE,
                    nodeHost, memUsageStr, memUsageMaxStr, memLimitStr});
            }

            return result;
        } else {

            ArrayResultCursor result = new ArrayResultCursor(LogicalShowProfileHandler.PROFILE_TABLE_NAME);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_STAGE, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_USED, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_PEAK, DataTypes.StringType);
            result.initMeta();

            ServerConnection targetSc = findTargetServerConnection(queryId);

            RuntimeStatistics runtimeStat = null;
            if (targetSc != null) {
                if (targetSc.isStatementExecuting().get()) {
                    ExecutionContext ec = targetSc.getTddlConnection().getExecutionContext();
                    if (profileIgnoreSqlTypeSet.contains(ec.getSqlType())) {
                        runtimeStat = targetSc.getLastSqlRunTimeStat();
                    } else {
                        runtimeStat = (RuntimeStatistics) targetSc.getTddlConnection()
                            .getExecutionContext()
                            .getRuntimeStatistics();
                    }
                } else {
                    runtimeStat = targetSc.getLastSqlRunTimeStat();
                }
            }

            if (runtimeStat != null) {
                String dbName = runtimeStat.getSchemaName();
                MemoryPool stmtPoolStat = runtimeStat.getMemoryPool();
                List<Object[]> rowInfos = new ArrayList<>();
                buildPoolInfoByMemoryStat(runtimeStat, dbName, rowInfos, stmtPoolStat, true, 0);
                for (int i = 0; i < rowInfos.size(); i++) {
                    result.addRow(rowInfos.get(i));
                }
            }
            return result;
        }
    }

    private void buildPoolInfoByMemoryStat(RuntimeStatistics runtimeStat, String dbName, List<Object[]> rowInfos,
                                           MemoryPool stmtPoolStat, boolean isOutputOptPool, int subQueryDepth) {

        if (stmtPoolStat == null || stmtPoolStat.getMaxMemoryUsage() <= 0) {
            return;
        }

        if (subQueryDepth == 0) {
            Object[] totalStat = new Object[] {
                LogicalShowProfileHandler.PROFILE_TOTAL,
                NUM_FORMAT.format(stmtPoolStat.getMemoryUsage()),
                NUM_FORMAT.format(stmtPoolStat.getMaxMemoryUsage())};
            rowInfos.add(totalStat);
        }

        if (isOutputOptPool) {
            List<AllocationStat> optimizerPoolAlloc = new ArrayList<>();
            boolean isCached = runtimeStat.getSqlWholeStageMemEstimation().cachedPlanMemEstimation != null;
            MemoryPool optimizerPoolStatOfStmt = null;
            if (!isCached) {
                optimizerPoolStatOfStmt = stmtPoolStat.getChildPool(MemoryType.PLANER.getExtensionName());
            }
            if (optimizerPoolStatOfStmt != null && optimizerPoolStatOfStmt.getMemoryUsageStat() > 0) {
                extractPoolInfo(optimizerPoolStatOfStmt, optimizerPoolAlloc, subQueryDepth);
                buildPoolInfoOutput(optimizerPoolStatOfStmt, isCached, optimizerPoolAlloc, rowInfos, subQueryDepth);
            }
        }

        if (!runtimeStat.getMemoryToStatistics().isEmpty()) {
            Object[] totalStat = new Object[] {
                "Server",
                LogicalShowProfileHandler.PROFILE_NO_VALUE,
                LogicalShowProfileHandler.PROFILE_NO_VALUE};
            rowInfos.add(totalStat);
            for (Map.Entry<String, MemoryStatisticsGroup> nodeEntry : runtimeStat.getMemoryToStatistics().entrySet()) {
                buildMppPoolInfoOutput(nodeEntry.getKey(), nodeEntry.getValue(), rowInfos, subQueryDepth);
            }
        } else {
            QueryMemoryPool queryMemoryPool = null;
            if (stmtPoolStat.getMemoryType() == MemoryType.QUERY) {
                queryMemoryPool = (QueryMemoryPool) stmtPoolStat;
            }
            if (queryMemoryPool != null) {
                if (queryMemoryPool != null && queryMemoryPool.getMaxMemoryUsage() > 0) {
                    List<AllocationStat> executionPoolAlloc = new ArrayList<>();
                    extractPoolInfo(queryMemoryPool, executionPoolAlloc, subQueryDepth);
                    buildPoolInfoOutput(queryMemoryPool, false, executionPoolAlloc, rowInfos, subQueryDepth);
                }

                String subQueryIndent =
                    StringUtils.repeat(LogicalShowProfileHandler.PROFILE_INDENTS_UNIT, subQueryDepth);
                for (MemoryPool memoryPool : queryMemoryPool.getChildren().values()) {
                    if (memoryPool.getMemoryType() == MemoryType.SUBQUERY) {
                        if (memoryPool.getMaxMemoryUsage() > 0) {
                            rowInfos
                                .add(new Object[] {
                                    subQueryIndent + memoryPool.getName(),
                                    NUM_FORMAT.format(memoryPool.getMemoryUsage()),
                                    NUM_FORMAT.format(memoryPool.getMaxMemoryUsage())});
                            buildPoolInfoByMemoryStat(runtimeStat, dbName, rowInfos, memoryPool, false,
                                subQueryDepth + 1);
                        }
                    }
                }
            }
        }
    }

    private ServerConnection findTargetServerConnection(String queryIdStr) {
        ServerConnection targetSc = null;
        Long queryId = Long.valueOf(queryIdStr);
        for (NIOProcessor p : CobarServer.getInstance().getProcessors()) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc != null && fc instanceof ServerConnection) {
                    ServerConnection sc = (ServerConnection) fc;
                    if (sc.getId() == queryId) {
                        targetSc = sc;
                        break;
                    }
                }
            }
        }
        return targetSc;
    }

    private void buildMppPoolInfoOutput(String name, MemoryStatisticsGroup memoryStatisticsGroup,
                                        List<Object[]> rowInfos, int depth) {
        String depthIndent = StringUtils.repeat(LogicalShowProfileHandler.PROFILE_INDENTS_UNIT, depth + 1);
        rowInfos.add(new Object[] {
            depthIndent + name,
            NUM_FORMAT.format(memoryStatisticsGroup.getMemoryUsage()),
            NUM_FORMAT.format(memoryStatisticsGroup.getMaxMemoryUsage()),
            LogicalShowProfileHandler.PROFILE_NO_VALUE});
        if (memoryStatisticsGroup.getMemoryStatistics() != null) {
            for (Map.Entry<String, MemoryStatisticsGroup> entry : memoryStatisticsGroup.getMemoryStatistics()
                .entrySet()) {
                buildMppPoolInfoOutput(entry.getKey(), entry.getValue(), rowInfos, depth + 1);
            }
        }
    }

    protected void buildPoolInfoOutput(MemoryPool rootPool, boolean isCacehed, List<AllocationStat> poolAllocList,
                                       List<Object[]> returnRows, int subQueryDepth) {
        if (rootPool == null) {
            return;
        }
        //String subQueryIndent = StringUtils.repeat(LogicalShowProfileHandler.PROFILE_INDENTS_UNIT, subQueryDepth);
        //树型结构不排序
        //Collections.sort(poolAllocList, MEMORY_ALLOC_COMP);
        String poolName = rootPool.getName();
        if (isCacehed) {
            poolName = poolName + LogicalShowProfileHandler.PROFILE_CACHED_POSTFIX;
        }
        //poolName = subQueryIndent + poolName;
        Object[] rowInfo = new Object[] {
            poolName,
            NUM_FORMAT.format(rootPool.getMemoryUsage()),
            NUM_FORMAT.format(rootPool.getMaxMemoryUsage())};
        returnRows.add(rowInfo);
        int size = poolAllocList.size();
        for (int i = 0; i < size; i++) {
            AllocationStat mai = poolAllocList.get(i);
            String allocId = LogicalShowProfileHandler.PROFILE_INDENTS_UNIT + mai.allocId;
            Object[] allocInfo = new Object[] {
                allocId,
                NUM_FORMAT.format(mai.usedSize),
                NUM_FORMAT.format(mai.usedPeak),
                LogicalShowProfileHandler.PROFILE_NO_VALUE};
            returnRows.add(allocInfo);
        }
    }

    protected void extractPoolInfo(MemoryPool poolStat, List<AllocationStat> pooAllocItemList, int depth) {
        if (poolStat == null) {
            return;
        }
        String treeIndent = StringUtils.repeat(LogicalShowProfileHandler.PROFILE_INDENTS_UNIT, depth);
        for (MemoryPool memoryPool : poolStat.getChildren().values()) {
            AllocationStat mai = new AllocationStat(treeIndent + memoryPool.getName(),
                memoryPool.getMemoryUsage(), memoryPool.getMaxMemoryUsage());
            pooAllocItemList.add(mai);
            if (memoryPool.getChildrenSize() > 0) {
                extractPoolInfo(memoryPool, pooAllocItemList, depth + 1);
            }
        }
    }

    protected ResultCursor syncForCpu() {

        if (queryId == null) {

            ArrayResultCursor result = new ArrayResultCursor(LogicalShowProfileHandler.PROFILE_TABLE_NAME);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_CONN_ID, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_TRACE_ID, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_DB, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_LOG_TIME_COST, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_PHY_SQL_TIME_COST, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_PHY_RS_TIME_COST, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_SQL, DataTypes.StringType);
            result.initMeta();

            PriorityQueue<CpuSketchItem> allSortedStmtCpuList = new PriorityQueue<>(CPU_COMP);
            for (NIOProcessor p : CobarServer.getInstance().getProcessors()) {
                for (FrontendConnection fc : p.getFrontends().values()) {
                    if (fc != null && fc instanceof ServerConnection) {
                        ServerConnection sc = (ServerConnection) fc;
                        if (sc.isStatementExecuting().get()) {
                            ExecutionContext ec = sc.getTddlConnection().getExecutionContext();
                            if (ExecUtils.isOperatorMetricEnabled(ec)) {
                                RuntimeStatistics runTimeStat = (RuntimeStatistics) ec.getRuntimeStatistics();
                                if (runTimeStat != null) {
                                    Metrics metrics = runTimeStat.toMetrics();

                                    CpuSketchItem cpuSketchItem = new CpuSketchItem();
                                    cpuSketchItem.queryId = sc.getId();
                                    cpuSketchItem.traceId = ec.getTraceId();
                                    cpuSketchItem.dbName = ec.getSchemaName();
                                    cpuSketchItem.logTc = metrics.logCpuTc;
                                    cpuSketchItem.phySqlTc = metrics.execSqlTc;
                                    cpuSketchItem.phyRsTc = metrics.fetchRsTc;

                                    String sqlContext = sc.getSqlSample();
                                    if (sqlContext != null) {
                                        cpuSketchItem.sql = TStringUtil.substring(sc.getSqlSample(), 0, 1000);
                                    } else {
                                        cpuSketchItem.sql = "";
                                    }
                                    allSortedStmtCpuList.add(cpuSketchItem);
                                }
                            }
                        }
                    }
                }
            }

            String logTcStr = "";
            String phySqlTcStr = "";
            String phyRsTcStr = "";
            while (!allSortedStmtCpuList.isEmpty()) {
                CpuSketchItem stmtCpuItem = allSortedStmtCpuList.poll();
                long queryId = stmtCpuItem.queryId;
                String traceId = stmtCpuItem.traceId;
                String dbName = stmtCpuItem.dbName;
                String sqlInfo = stmtCpuItem.sql;
                long logTc = stmtCpuItem.logTc;
                long phySqlTc = stmtCpuItem.phySqlTc;
                long phyRsTc = stmtCpuItem.phyRsTc;
                logTcStr = NUM_FORMAT.format(logTc);
                phySqlTcStr = NUM_FORMAT.format(phySqlTc);
                phyRsTcStr = NUM_FORMAT.format(phyRsTc);
                result.addRow(new Object[] {queryId, traceId, dbName, logTcStr, phySqlTcStr, phyRsTcStr, sqlInfo});
            }

            return result;
        } else {
            ArrayResultCursor result = new ArrayResultCursor(LogicalShowProfileHandler.PROFILE_TABLE_NAME);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_STAGE, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_TIME_COST, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_TIME_COST_PCT, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_ROW_COUNT, DataTypes.StringType);
            result.initMeta();

            ServerConnection targetSc = findTargetServerConnection(queryId);
            ExecutionContext ec = null;
            CpuStat cpuStatManager = null;
            RuntimeStatistics runTimeStat = null;
            RelNode planTree = null;
            long totalTc = 0;

            if (targetSc != null) {
                if (targetSc.isStatementExecuting().get()) {
                    ec = targetSc.getTddlConnection().getExecutionContext();
                    if (profileIgnoreSqlTypeSet.contains(ec.getSqlType())) {
                        runTimeStat = targetSc.getLastSqlRunTimeStat();
                    } else {
                        runTimeStat = (RuntimeStatistics) targetSc.getTddlConnection()
                            .getExecutionContext()
                            .getRuntimeStatistics();
                    }
                } else {
                    runTimeStat = targetSc.getLastSqlRunTimeStat();
                }

                if (runTimeStat != null) {
                    cpuStatManager = runTimeStat.getSqlWholeStageCpuStat();
                    planTree = runTimeStat.getPlanTree();
                    if (planTree != null && planTree instanceof BaseTableOperation && !runTimeStat
                        .isFromAllAtOnePhyTable()) {
                        planTree = ((BaseTableOperation) planTree).getParent();
                    }
                }
            }
            if (runTimeStat != null && runTimeStat.isRunningWithCpuProfile()) {
                Metrics metrics = runTimeStat.toMetrics();
                totalTc = metrics.totalTc;
                if (cpuStatManager == null) {
                    cpuStatManager = runTimeStat.getSqlWholeStageCpuStat();
                }

                // Process CpuStatItem
                List<CpuStatItem> cpuStatItems = cpuStatManager.getStatItems();
                List<CpuStatAttr> topAttrList = CpuStatAttribute.getChildrenAttr(CpuStatAttr.ROOT);
                for (int i = 0; i < topAttrList.size(); i++) {
                    CpuStatAttr cpuStatAttr = topAttrList.get(i);
                    processCpuStatItems(cpuStatItems, cpuStatAttr, totalTc, 0, result);
                }

                // Process Root of ExecutePlan
                String titleOfPlanOperators = CpuStatAttr.PLAN_EXECUTION.getAttrName();
                boolean isPlanFinishedExecuting = runTimeStat.isFinishExecution();
                if (isPlanFinishedExecuting) {
                    String extMsg = String.format(LogicalShowProfileHandler.PROFILE_TRACE_ID,
                        runTimeStat.getTraceId(),
                        LogicalShowProfileHandler.PROFILE_PLAN_STATE_COMPLETED);
                    titleOfPlanOperators += extMsg;
                } else {
                    String extMsg = String.format(LogicalShowProfileHandler.PROFILE_TRACE_ID,
                        runTimeStat.getTraceId(),
                        LogicalShowProfileHandler.PROFILE_PLAN_STATE_RUNNING);
                    titleOfPlanOperators += extMsg;
                }
                Object[] tmpRowInfo = new Object[] {
                    titleOfPlanOperators, String.valueOf(""), String.valueOf(""),
                    String.valueOf("")};
                result.addRow(tmpRowInfo);
                if (SqlTypeUtils.isSelectSqlType(runTimeStat.getSqlType())) {
                    // Process Plan Operators
                    Object[] rowInfo = null;
                    List<RelNodeItem> relNodeItemList = new ArrayList<>();
                    if (planTree != null) {
                        RelItemVisitor relItemVisitor = new RelItemVisitor(runTimeStat);
                        relItemVisitor.go(planTree);
                        relNodeItemList = relItemVisitor.getRelNodeItemList();
                    }

                    for (int i = 0; i < relNodeItemList.size(); i++) {
                        RelNodeItem relNodeItem = relNodeItemList.get(i);
                        String tcStr = LogicalShowProfileHandler.PROFILE_VALUE_COMPUTING;
                        String pctStr = LogicalShowProfileHandler.PROFILE_VALUE_COMPUTING;
                        long itemTc = relNodeItem.timeCost;
                        if (itemTc >= 0 && totalTc > 0) {
                            double pct = 100.0 * itemTc / totalTc;
                            tcStr = NUM_FORMAT.format(itemTc);
                            if (pct <= 100.0) {
                                pctStr = String.format("%.4f%%", pct);
                            }
                        } else if (totalTc == 0) {
                            tcStr = LogicalShowProfileHandler.PROFILE_ZEOR_VALUE;
                            pctStr = LogicalShowProfileHandler.PROFILE_ZEOR_PCT_VALUE;
                        }
                        String relNodeName = relNodeItem.relNodeName;
                        if (relNodeItem.parallelism > 1) {
                            relNodeName += String.format(LogicalShowProfileHandler.PROFILE_PLAN_PARALLELISM,
                                relNodeItem.parallelism);
                        }
                        rowInfo = new Object[] {
                            relNodeName,
                            tcStr,
                            pctStr,
                            relNodeItem.rowCount < 0 ? LogicalShowProfileHandler.PROFILE_NO_VALUE :
                                String.valueOf(relNodeItem.rowCount)};
                        result.addRow(rowInfo);
                    }
                }
                tmpRowInfo = new Object[] {
                    CpuStatAttribute.LOGICAL_TIME_COST, NUM_FORMAT.format(metrics.logCpuTc),
                    String.format("%.4f%%", 100.0 * metrics.logCpuTc / metrics.totalTc),
                    LogicalShowProfileHandler.PROFILE_NO_VALUE};
                result.addRow(tmpRowInfo);

                tmpRowInfo = new Object[] {
                    CpuStatAttribute.PHYSICAL_TIME_COST, NUM_FORMAT.format(metrics.phyCpuTc),
                    String.format("%.4f%%", 100.0 * metrics.phyCpuTc / metrics.totalTc),
                    LogicalShowProfileHandler.PROFILE_NO_VALUE};
                result.addRow(tmpRowInfo);

                tmpRowInfo = new Object[] {
                    CpuStatAttribute.TOTAL_TIME_COST, NUM_FORMAT.format(metrics.totalTc),
                    String.format("%.4f%%", 100.0 * metrics.totalTc / metrics.totalTc),
                    LogicalShowProfileHandler.PROFILE_NO_VALUE};
                result.addRow(tmpRowInfo);

            }

            return result;
        }
    }

    protected void processCpuStatItems(List<CpuStatItem> cpuStatItems, CpuStatAttr currStatAttr, long totalTc,
                                       int level, ArrayResultCursor result) {
        List<CpuStatAttr> childrenAttrs = CpuStatAttribute.getChildrenAttr(currStatAttr);
        if (childrenAttrs != null && childrenAttrs.size() > 0) {
            for (int i = 0; i < childrenAttrs.size(); i++) {
                CpuStatAttr childAttr = childrenAttrs.get(i);
                processCpuStatItems(cpuStatItems, childAttr, totalTc, level + 1, result);
            }
        }

        CpuStatItem cpuStatItem = cpuStatItems.get(currStatAttr.getAttrId());
        if (cpuStatItem != null) {
            StringBuilder indentSb = new StringBuilder("");
            for (int i = 1; i < level; i++) {
                indentSb.append(LogicalShowProfileHandler.PROFILE_INDENTS_UNIT);
            }
            double itemTc = cpuStatItem.timeCostNano;
            double pct = 100 * itemTc / totalTc;
            String pctStr = String.format("%.4f%%", pct);
            String tcStr = NUM_FORMAT.format(cpuStatItem.timeCostNano);
            Object[] rowInfo = new Object[] {
                indentSb.append(currStatAttr.getAttrName()), tcStr, pctStr,
                LogicalShowProfileHandler.PROFILE_NO_VALUE};
            result.addRow(rowInfo);
        }
        return;
    }

    @Override
    public ResultCursor sync() {
        String type = SqlShowProfile.CPU_TYPE;
        if (types != null) {
            type = types.get(0);
        }
        if (type.equalsIgnoreCase(SqlShowProfile.MEMORY_TYPE)) {
            return syncForMemory();
        } else {
            return syncForCpu();
        }

    }
}
