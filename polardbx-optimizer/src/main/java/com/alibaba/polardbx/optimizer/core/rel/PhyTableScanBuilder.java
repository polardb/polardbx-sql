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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.PruneRawString;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.jdbc.UnionBytesSql;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.convertor.ConvertorHelper;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.convertor.ConvertorHelper;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.convertor.ConvertorHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.Xplan.XPlanTemplate;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.SpecialFunctionRelFinder;
import com.alibaba.polardbx.optimizer.core.rel.util.DynamicParamInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.IndexedDynamicParamInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.RuntimeFilterDynamicParamInfo;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.optimizer.utils.TargetTableInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.sql.SqlKind.PLUS;

/**
 * 构建 PhyTableOperation, 做 UNION 优化
 * <p>
 *
 * @author lingce.ldm 2017-11-15 14:00
 */
public class PhyTableScanBuilder extends PhyOperationBuilderCommon {

    /**
     * <p>
     * If unionSize <= 0, union all sql to one; else union unionSize sql to one. Use
     * merge_union_size hint can set it.
     * </p>
     */
    protected int unionSize = 1;

    /**
     * SQL 模板,表名已经被参数化
     */

    protected final SqlSelect sqlTemplate;

    /**
     * <pre>
     * key: GroupName
     * values: List of TableNames
     * </pre>
     */
    protected Map<String, List<List<String>>> targetTables;
    /**
     * SQL 参数
     */
    protected final Map<Integer, ParameterContext> params;
    protected final RelNode parent;
    protected final DbType dbType;
    protected final List<DynamicParamInfo> dynamicParamList;
    protected final RelDataType rowType;
    protected final String schemaName;
    protected final List<String> logicalTableNames;
    protected UnionOptHelper unionOptHelper;
    protected ExecutionContext executionContext;
    protected boolean buildForPushDownOneShardOnly = false;

    public PhyTableScanBuilder(SqlSelect sqlTemplate, Map<String, List<List<String>>> targetTables,
                               ExecutionContext executionContext, RelNode parent, DbType dbType,
                               RelDataType rowType, String schemaName, List<String> logicalTableNames,
                               boolean useCache) {
        this.executionContext = executionContext;
        this.targetTables = targetTables;
        this.params = executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter();
        this.parent = parent;
        this.dbType = dbType;
        this.rowType = rowType;

        boolean usingPhySqlCache = executionContext.enablePhySqlCache() & useCache;
        if (usingPhySqlCache &&
            (executionContext.getCorrelateFieldInViewMap() == null || executionContext
                .getCorrelateFieldInViewMap().isEmpty())) {
            this.sqlTemplate = sqlTemplate;
            this.sqlTemplate.accept(new FetchPreprocessor(params, true));
        } else {
            this.sqlTemplate = (SqlSelect) sqlTemplate.accept(
                new ReplaceTableNameWithSomethingVisitor(executionContext.getCorrelateFieldInViewMap(),
                    schemaName,
                    executionContext) {
                    @Override
                    protected SqlNode buildSth(SqlNode sqlNode) {
                        return sqlNode;
                    }
                }
            );
            this.sqlTemplate.accept(new FetchPreprocessor(params, false));
        }
        this.dynamicParamList = PlannerUtils.getDynamicParamInfoList(this.sqlTemplate);
        this.schemaName = schemaName;
        this.logicalTableNames = logicalTableNames;
    }

    public PhyTableScanBuilder(SqlSelect sqlTemplate, Map<String, List<List<String>>> targetTables,
                               ExecutionContext executionContext, RelNode parent, DbType dbType, String schemaName,
                               List<String> logicalTableName) {
        this(sqlTemplate, targetTables, executionContext, parent, dbType, parent.getRowType(), schemaName,
            logicalTableName, true);
    }

    public PhyTableScanBuilder(SqlSelect sqlTemplate, Map<String, List<List<String>>> targetTables,
                               ExecutionContext executionContext, RelNode parent, DbType dbType, String schemaName,
                               List<String> logicalTableName, boolean useCache) {
        this(sqlTemplate, targetTables, executionContext, parent, dbType, parent.getRowType(), schemaName,
            logicalTableName, useCache);
    }

    private static class FetchPreprocessor extends SqlShuttle {

        protected final Map<Integer, ParameterContext> params;
        private final boolean usingCache;

        private FetchPreprocessor(Map<Integer, ParameterContext> params, boolean usingCache) {
            this.params = params;
            this.usingCache = usingCache;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            final SqlNode visited = super.visit(call);

            if (visited instanceof SqlSelect) {
                preProcessFetch((SqlSelect) visited);
            }

            return visited;
        }

        /**
         * If the limit like {@code limit ? +?}, it is a PLUS function and with param,
         * we calculate it, MySQL DO NOT support this format.
         */
        private void preProcessFetch(SqlSelect sqlTemplate) {
            SqlNode fetch = sqlTemplate.getFetch();
            if (fetch == null) {
                return;
            }

            if (fetch instanceof SqlLiteral || fetch instanceof SqlDynamicParam) {
                return;
            }

            if (fetch.getKind() == PLUS) {
                long fetchVal = computeFetchValue((SqlCall) fetch);
                if (fetchVal == -1) {
                    return;
                }

                if (usingCache) {
                    /*
                      We have to parameterize the limit due to LogicalView#sqlTemplateStringCache.
                     */
                    SqlDynamicParam dynamicParam = new SqlDynamicParam(params.size(),
                        SqlTypeName.BIGINT,
                        SqlParserPos.ZERO,
                        null);
                    // it is ok to set concurrently
                    sqlTemplate.setComputedFetch(dynamicParam);
                    // put the computed value to params in execution context
                    params.put(params.size() + 1, new ParameterContext(
                        OptimizerUtils.getParameterMethod(fetchVal), new Object[] {params.size() + 1, fetchVal}));
                } else {
                    /*
                      When no cache, we can generate a Literal directly.
                      Set the new Fetch value. For native sql, we do not parameterized the limit
                      value.
                     */
                    sqlTemplate
                        .setFetch(SqlLiteral.createExactNumeric(String.valueOf(fetchVal), fetch.getParserPosition()));
                }
            }
        }

        private long computeFetchValue(SqlCall fetch) {
            long fetchVal = 0;
            /**
             * Compute the new FETCH value.
             */
            for (SqlNode op : fetch.getOperandList()) {
                if (op instanceof SqlDynamicParam) {
                    if (params == null) {
                        return -1;
                    }
                    int index = ((SqlDynamicParam) op).getIndex();
                    fetchVal = fetchVal + Long.valueOf(String.valueOf(params.get(index + 1).getValue())).longValue();
                } else if (op instanceof SqlLiteral) {
                    fetchVal = fetchVal + ((SqlLiteral) op).longValue(false);
                } else {
                    // Impossible.
                    throw new TddlNestableRuntimeException("Impossible be here.");
                }
            }
            return fetchVal;
        }
    }

    public List<RelNode> build(ExecutionContext executionContext) {
        convertParameters(this.params, executionContext);

        List<RelNode> allPhyTableScans = new ArrayList<>();
        CursorMeta cursorMeta = CursorMeta.build(CalciteUtils.buildColumnMeta(rowType, "TableScan"));

        final XPlanTemplate XPlan;
        final BytesSql bytesSql;
        if (parent instanceof LogicalView && ((LogicalView) parent).getSqlTemplate(executionContext) == sqlTemplate) {
            bytesSql = ((LogicalView) parent).getBytesSql(sqlTemplate);
            XPlan = ((LogicalView) parent).getXPlan();
        } else {
            bytesSql = RelUtils.toNativeBytesSql(sqlTemplate, dbType);
            XPlan = null;
        }

        ByteString sqlTemplateDigest = null;
        // Init sql digest.
        try {
            sqlTemplateDigest = bytesSql.digest();
        } catch (Exception ignore) {
        }

        // prepare GP digest
        final ByteString galaxyPrepareDigest = parent instanceof LogicalView ?
            ((LogicalView) parent).getGalaxyPrepareDigest(executionContext, bytesSql) :
            Planner.calcGalaxyPrepareDigest(schemaName, bytesSql, logicalTableNames, executionContext);
        final boolean supportGalaxyPrepare;
        if (null == galaxyPrepareDigest) {
            supportGalaxyPrepare = false;
        } else if (parent instanceof LogicalView) {
            supportGalaxyPrepare = ((LogicalView) parent).isSupportGalaxyPrepare();
        } else {
            final SpecialFunctionRelFinder finder = new SpecialFunctionRelFinder();
            finder.go(parent);
            supportGalaxyPrepare = finder.supportGalaxyPrepare();
        }

        if (parent instanceof LogicalView) {
            LogicalView lv = (LogicalView) parent;
            boolean isNeedPrune = isNeedPrune((LogicalView) parent, executionContext);
            if (isNeedPrune) {
                /**
                 * prune step 1: find all raw strings
                 */
                Map<Integer, RawString> rawStrings = findAllRawStrings(executionContext);

                // step 2: prune args(rawstring)
                int step = executionContext.getParamManager().getInt(ConnectionParams.IN_PRUNE_STEP_SIZE);
                int maxPruneTime = executionContext.getParamManager().getInt(ConnectionParams.IN_PRUNE_MAX_TIME);
                if (rawStrings.size() == 1) {
                    RawString r = rawStrings.values().iterator().next();
                    step = r.size() / maxPruneTime > step ? r.size() / maxPruneTime : step;
                    int currenteIndex = 0;
                    Map<Pair<String, List<String>>, Parameters> pruneRawStringMap = Maps.newHashMap();
                    while (currenteIndex < r.size()) {
                        Parameters parameters = executionContext.getParams().clone();

                        // split rawstring ( args of in expr)
                        PruneRawString p =
                            new PruneRawString(r.getObjList(), PruneRawString.PRUNE_MODE.RANGE, currenteIndex,
                                (currenteIndex + step) > r.size() ? r.size() : (currenteIndex + step),
                                null);

                        // sub parameter
                        ParameterContext parameterContext = new ParameterContext();
                        Object[] objs = new Object[2];
                        objs[0] = rawStrings.keySet().iterator().next();
                        objs[1] = p;
                        parameterContext.setArgs(objs);
                        parameterContext.setParameterMethod(ParameterMethod.setObject1);
                        parameters.getCurrentParameter().put(rawStrings.keySet().iterator().next(), parameterContext);

                        // sub context
                        ExecutionContext e = executionContext.copy(parameters);

                        // cal prune
                        Map<String, List<List<String>>> groupAndTables = lv.buildTargetTables(e);

                        for (Map.Entry<String, List<List<String>>> entry1 : groupAndTables.entrySet()) {
                            String group = entry1.getKey();
                            List<List<String>> tables = entry1.getValue();
                            for (List<String> tbls : tables) {
                                Pair<String, List<String>> pairs = new Pair<>(group, tbls);
                                if (pruneRawStringMap.get(pairs) == null) {
                                    pruneRawStringMap.put(pairs, parameters);
                                } else {
                                    pruneRawStringMap.put(pairs,
                                        mergeRawStringParameters(parameters, pruneRawStringMap.get(pairs)));
                                }
                            }
                        }
                        currenteIndex += step;
                    }

                    executionContext.setPruneRawStringMap(pruneRawStringMap);
                }
            }
        }

        int totalCount = 0;
        for (Map.Entry<String, List<List<String>>> t : targetTables.entrySet()) {
            totalCount += t.getValue().size();
        }

        String schemaName = this.schemaName;
        List<String> logTblNames = this.logicalTableNames;
        Map<String, List<List<String>>> tmpTargetTables = targetTables;
        SqlSelect.LockMode lockMode = sqlTemplate.getLockMode();
        boolean isForUpdate =
            lockMode == SqlSelect.LockMode.EXCLUSIVE_LOCK || lockMode == SqlSelect.LockMode.SHARED_LOCK;


        Boolean enableGrpParallelism =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_GROUP_PARALLELISM);
        if (enableGrpParallelism) {
            TargetTableInfo targetTableInfo =
                PhyTableOperationUtil.groupTargetTablesByGroupConnId(schemaName, logTblNames, targetTables, isForUpdate,
                    executionContext);
            ShardPlanMemoryContext shardPlanMemoryContext = buildShardPlanMemoryContext(parent,
                bytesSql,
                (AbstractRelNode) parent,
                this.params,
                tmpTargetTables,
                this.executionContext);
            for (Map<String, List<List<String>>> targeTableItemsOfOneGrpConnId : targetTableInfo.getGrpConnIdTargetTablesMap()
                .values()) {
                List<RelNode> phyTableScans =
                    buildPhyScanOp(cursorMeta, bytesSql, XPlan, sqlTemplateDigest, supportGalaxyPrepare,
                        galaxyPrepareDigest, targeTableItemsOfOneGrpConnId, totalCount, shardPlanMemoryContext);
                allPhyTableScans.addAll(phyTableScans);
            }
        } else {
            ShardPlanMemoryContext shardPlanMemoryContext = buildShardPlanMemoryContext(parent,
                bytesSql,
                (AbstractRelNode) parent,
                this.params,
                tmpTargetTables,
                this.executionContext);
            List<RelNode> phyTableScans =
                buildPhyScanOp(cursorMeta, bytesSql, XPlan, sqlTemplateDigest, supportGalaxyPrepare,
                    galaxyPrepareDigest, tmpTargetTables, totalCount, shardPlanMemoryContext);
            allPhyTableScans.addAll(phyTableScans);
        }

        if (buildForPushDownOneShardOnly && allPhyTableScans.size() > 1) {
            throw new IllegalArgumentException(
                "Invalid build params of PhyTableOperation: buildForPushDownOneShardOnly="
                    + buildForPushDownOneShardOnly);
        }
        return allPhyTableScans;
    }

    private List<RelNode> buildPhyScanOp(CursorMeta cursorMeta, BytesSql bytesSql,XPlanTemplate XPlan, ByteString sqlTemplateDigest,
                                        boolean supportGalaxyPrepare, ByteString galaxyPrepareDigest, Map<String, List<List<String>>> tmpTargetTables,
                                         int totalPhyTblCnt, ShardPlanMemoryContext shardPlanMemoryContext) {
        List<RelNode> phyTableScans = new ArrayList<>();
        MemoryAllocatorCtx maOfPlanBuildingPool = shardPlanMemoryContext.memoryAllocator;
        long phyOpMemSize = shardPlanMemoryContext.phyOpMemSize;

        for (Map.Entry<String, List<List<String>>> t : tmpTargetTables.entrySet()) {
            String group = t.getKey();
            List<List<String>> tableNames = t.getValue();
            int realUnionSize =
                unionOptHelper != null ? unionOptHelper.calMergeUnionSize(totalPhyTblCnt, tableNames.size(), group) :
                    unionOptHelper != null ?
                        unionOptHelper.calMergeUnionSize(totalPhyTblCnt, tableNames.size(), group) :
                        unionSize;
            if (realUnionSize <= 0) {
                /**
                 * UNION all native sql at one group.
                 */
                if (maOfPlanBuildingPool != null) {
                    maOfPlanBuildingPool.allocateReservedMemory(phyOpMemSize);
                }
                PhyTableOperation phyTableOp = buildOnePhyTableOperatorForScan(group,
                    tableNames,
                    cursorMeta,
                    bytesSql,
                    1,
                    rowType,
                    XPlan,
                    sqlTemplateDigest,
                    supportGalaxyPrepare,
                    galaxyPrepareDigest,
                    maOfPlanBuildingPool);

//                phyTableOp.setLogicalTableNames(logicalTableNames);
//                phyTableOp.setXTemplate(XPlan);
//                phyTableOp.setSqlDigest(sqlTemplateDigest);

                phyTableScans.add(phyTableOp);

            } else {
                for (int i = 0; i < tableNames.size(); ) {
                    int endIndex = i + realUnionSize;
                    endIndex = endIndex > tableNames.size() ? tableNames.size() : endIndex;
                    List<List<String>> subTableNames = tableNames.subList(i, endIndex);

                    if (maOfPlanBuildingPool != null) {
                        maOfPlanBuildingPool.allocateReservedMemory(phyOpMemSize);
                    }
                    PhyTableOperation phyTableOp = buildOnePhyTableOperatorForScan(group,
                        subTableNames,
                        cursorMeta,
                        bytesSql,
                        realUnionSize,
                        rowType,
                        XPlan,
                        sqlTemplateDigest,
                        supportGalaxyPrepare,
                        galaxyPrepareDigest,
                        maOfPlanBuildingPool);
//                    phyTableOp.setLogicalTableNames(logicalTableNames);
//                    phyTableOp.setXTemplate(XPlan);
//                    phyTableOp.setSqlDigest(sqlTemplateDigest);

                    phyTableScans.add(phyTableOp);
                    i = endIndex;
                }
            }
        }
        return phyTableScans;
    }

    private Parameters mergeRawStringParameters(Parameters parameterContexts,
                                                Parameters parameterContexts1) {
        Map<Integer, ParameterContext> map = parameterContexts.getCurrentParameter();
        for (Map.Entry<Integer, ParameterContext> entry : map.entrySet()) {
            if (entry.getValue() != null && entry.getValue().getValue() instanceof PruneRawString) {
                PruneRawString pruneRawString = (PruneRawString) entry.getValue().getValue();
                PruneRawString pruneRawString1 =
                    (PruneRawString) parameterContexts1.getCurrentParameter().get(entry.getKey()).getValue();
                pruneRawString.merge(pruneRawString1);
            }
        }
        return new Parameters(map);
    }

    private Map<Integer, RawString> findAllRawStrings(ExecutionContext executionContext) {
        Map<Integer, RawString> allRawString = Maps.newHashMap();
        if (executionContext.getParams() != null) {
            Map<Integer, ParameterContext> parameterContextMap = executionContext.getParams().getCurrentParameter();
            for (Map.Entry<Integer, ParameterContext> entry : parameterContextMap.entrySet()) {
                if (entry.getValue() != null && entry.getValue().getValue() instanceof RawString) {
                    RawString rawString = (RawString) entry.getValue().getValue();
                    int pruneSize = executionContext.getParamManager().getInt(ConnectionParams.IN_PRUNE_SIZE);
                    if (rawString.size() > pruneSize) {
                        allRawString.put(entry.getKey(), rawString);
                    }
                }
            }
        }
        return allRawString;
    }

    /**
     * <pre>
     * 兼容mysql5.6的时间精度,去掉毫秒.针对部分单表下推的sql在优化器层无法处理,只能在执行层做一次时间精度处理了
     * </pre>
     */
    public void convertParameters(Map<Integer, ParameterContext> params, ExecutionContext executionContext) {
        boolean enableCompatibleDatetimeRoundDown =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_COMPATIBLE_DATETIME_ROUNDDOWN);
        boolean enableCompatibleTimestampRoundDown =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_COMPATIBLE_TIMESTAMP_ROUNDDOWN);

        if (enableCompatibleDatetimeRoundDown || enableCompatibleTimestampRoundDown) {
            for (ParameterContext paramContext : params.values()) {
                Object value = paramContext.getValue();
                if (value instanceof Date) {
                    long mills = ((Date) value).getTime();
                    if (mills % 1000 > 0) {
                        // 去掉精度
                        paramContext.setValue(ConvertorHelper.longToDate.convert(((mills / 1000) * 1000),
                            value.getClass()));
                    }
                }
            }
        }
        return;
    }

    public List<ParameterContext> buildParams(String group, List<List<String>> tableNames) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(tableNames));

        List<ParameterContext> results = new ArrayList<>();
        for (List<String> ts : tableNames) {
            Preconditions.checkArgument(CollectionUtils.isNotEmpty(ts));
            results.addAll(buildSplitParams(group, ts, false));
        }
        return results;
    }

    public List<ParameterContext> buildSplitParams(String group, List<String> tables, boolean useDelegate) {
        List<ParameterContext> results = new ArrayList<>();
        Map<Integer, ParameterContext> currentParams = executionContext.getPruneParams(group, tables);
        int tableIndex = -1;
        for (DynamicParamInfo dynamicParamInfo : dynamicParamList) {
            if (dynamicParamInfo instanceof IndexedDynamicParamInfo) {
                int i = ((IndexedDynamicParamInfo) dynamicParamInfo).getParamIndex();
                if (i == PlannerUtils.TABLE_NAME_PARAM_INDEX) {
                    tableIndex += 1;
                    results.add(PlannerUtils.buildParameterContextForTableName(tables.get(tableIndex), 0));
                } else if (i == PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX) {
                    int dynamicKey = ((IndexedDynamicParamInfo) dynamicParamInfo).getDynamicKey();
                    if (dynamicKey != -1) {
                        ParameterContext parameterContext = new ParameterContext();
                        parameterContext.setParameterMethod(ParameterMethod.setObject1);
                        Object[] args = new Object[2];
                        args[1] = executionContext.getScalarSubqueryVal(dynamicKey);
                        parameterContext.setArgs(args);
                        results.add(parameterContext);
                    }
                    // do nothing
                } else if (i == PlannerUtils.APPLY_SUBQUERY_PARAM_INDEX) {
                    // do nothing
                } else {
                    if (currentParams != null) {
                        results.add(currentParams.get(i + 1));
                    } else {
                        if (useDelegate) {
                            results.add(new ParameterContext(ParameterMethod.setDelegate, new Object[] {i + 1}));
                        } else {
                            results.add(params.get(i + 1));
                        }
                    }
                }
            } else if (dynamicParamInfo instanceof RuntimeFilterDynamicParamInfo) {
                results.add(((RuntimeFilterDynamicParamInfo) dynamicParamInfo).toParameterContext());
            } else {
                throw new IllegalArgumentException("Unsupported dynamic param info: " + dynamicParamInfo);
            }
        }
        return results;
    }

    public Map<Integer, ParameterContext> buildSplitParamMap(List<String> tables) {
        Map<Integer, ParameterContext> results = new HashMap<>();
        int tableIndex = -1;
        for (DynamicParamInfo dynamicParamInfo : dynamicParamList) {
            if (dynamicParamInfo instanceof IndexedDynamicParamInfo) {
                int i = ((IndexedDynamicParamInfo) dynamicParamInfo).getParamIndex();
                if (i == PlannerUtils.TABLE_NAME_PARAM_INDEX) {
                    tableIndex += 1;
                    results.put(i, PlannerUtils.buildParameterContextForTableName(tables.get(tableIndex), 0));
                } else if (i == PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX) {
                    // do nothing
                } else if (i == PlannerUtils.APPLY_SUBQUERY_PARAM_INDEX) {
                    // do nothing
                } else {
                    results.put(i, this.params.get(i + 1));
                }
            } else {
                throw new IllegalArgumentException("Unsupported dynamic param info: " + dynamicParamInfo);
            }
        }
        return results;
    }

    public void setUnionSize(int unionSize) {
        this.unionSize = unionSize;
    }

    public void setUnionOptHelper(UnionOptHelper unionOptHelper) {
        this.unionOptHelper = unionOptHelper;
    }

    protected PhyTableOperation buildOnePhyTableOperatorForScan(String group,
                                                                List<List<String>> tableNames,
                                                                CursorMeta cursorMeta,
                                                                BytesSql bytesSql,
                                                                int realUnionSize,
                                                                RelDataType rowType,
                                                                XPlanTemplate xPlan,
                                                                ByteString sqlTemplateDigest,
                                                                boolean supportGalaxyPrepare,
                                                                ByteString galaxyPrepareDigest,
                                                                MemoryAllocatorCtx maOfPlanBuildingPool) {
        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setSchemaName(schemaName);
        buildParams.setLogTables(logicalTableNames);
        buildParams.setGroupName(group);
        buildParams.setPhyTables(tableNames);
        buildParams.setSqlKind(SqlKind.SELECT);
        buildParams.setLockMode(sqlTemplate.getLockMode());
        buildParams.setOnlyOnePartitionAfterPruning(buildForPushDownOneShardOnly);

        buildParams.setLogicalPlan(parent);
        buildParams.setCluster(parent.getCluster());
        buildParams.setTraitSet(parent.getTraitSet());
        buildParams.setRowType(rowType);
        buildParams.setCursorMeta(cursorMeta);
        buildParams.setNativeSqlNode(sqlTemplate);

        buildParams.setxTemplate(xPlan);
        buildParams.setSqlDigest(sqlTemplateDigest);
        buildParams.setSupportGalaxyPrepare(supportGalaxyPrepare);
        buildParams.setGalaxyPrepareDigest(galaxyPrepareDigest);
        buildParams.setMemoryAllocator(maOfPlanBuildingPool);
        buildParams.setBuilderCommon(this);
        buildParams.setUnionSize(realUnionSize);

        buildParams.setBytesSql(bytesSql);
        buildParams.setDbType(dbType);
        buildParams.setDynamicParams(params);
        buildParams.setBatchParameters(null);

        return PhyTableOperationFactory.getInstance().buildPhyTblOpByParams(buildParams);
    }

    public Map<Integer, ParameterContext> getParams() {
        return params;
    }

    public BytesSql buildBytesSql(PhyTableOperation phyTableOp) {
        int unionSize = phyTableOp.getTableNames().size();
        String orderBy = buildPhysicalOrderByClause();
        Preconditions.checkArgument(unionSize > 0, "The number of tables must great than 0 when build UNION ALL sql");
        if (unionSize == 1) {
            return phyTableOp.getBytesSql();
        }
        return new UnionBytesSql(phyTableOp.getBytesSql().getBytesArray(), phyTableOp.getBytesSql().isParameterLast(),
            unionSize, orderBy == null ? null : orderBy.getBytes(),
            null);
    }

    /**
     * <pre>
     * 在最外层添加 OrderBy, OrderBy 部分直接从 SQL 模板中获取,但直接使用会有如下问题:
     * 1. Order by 的列带有表名,该表名对于 UNION ALL 之后的结果是不适用的
     * 2. Order by 列名, sqlTemplate 中均 OrderBy 列名而非别名,导致外部 Order by 找不到列名
     * </pre>
     */
    public String buildPhysicalOrderByClause() {
        SqlNodeList selectList = sqlTemplate.getSelectList();
        SqlNodeList orderBy = sqlTemplate.getOrderList();
        if (orderBy == null) {
            return null;
        }

        SqlNodeList newOrder;
        // select * 不存在列名的问题
        Map<String, String> projectMap = new HashMap<>();
        if (selectList != null) {
            // 替换 OrderBy 中的原始列名为别名
            for (SqlNode selectNode : selectList) {
                if (selectNode.getKind() == SqlKind.AS) {
                    SqlNode[] operands = ((SqlBasicCall) selectNode).getOperands();
                    String key = operands[0].toString();
                    String value = Util.last(((SqlIdentifier) operands[1]).names);
                    projectMap.put(key, value);
                }
            }
        }
        newOrder = new SqlNodeList(orderBy.getParserPosition());
        for (SqlNode node : orderBy) {
            newOrder.add(RelUtils.convertColumnName(node, projectMap));
        }

        return RelUtils.toNativeSql(newOrder);
    }

    public boolean containLimit() {
        return sqlTemplate.getFetch() != null || sqlTemplate.getOffset() != null;
    }

    private boolean isNeedPrune(LogicalView logicalView, ExecutionContext executionContext) {
        if (logicalView.hasDynamicPruning() && executionContext.getParams() != null) {
            Map<Integer, ParameterContext> parameterContextMap = executionContext.getParams().getCurrentParameter();
            boolean needPrune = false;
            for (ParameterContext parameterContext : parameterContextMap.values()) {
                if (parameterContext != null && parameterContext.getValue() instanceof RawString) {
                    RawString rawString = (RawString) parameterContext.getValue();
                    int pruneSize = executionContext.getParamManager().getInt(ConnectionParams.IN_PRUNE_SIZE);
                    int pruneStep = executionContext.getParamManager().getInt(ConnectionParams.IN_PRUNE_STEP_SIZE);
                    int maxPruneTime = executionContext.getParamManager().getInt(ConnectionParams.IN_PRUNE_MAX_TIME);
                    if (rawString.size() > pruneStep * maxPruneTime) {
                        return false;
                    }
                    if (rawString.size() > pruneSize) {
                        needPrune = true;
                    }
                }
            }
            return needPrune;
        }
        return false;
    }

    public void setBuildForPushDownOneShardOnly(boolean buildForPushDownOneShardOnly) {
        this.buildForPushDownOneShardOnly = buildForPushDownOneShardOnly;
    }
}
