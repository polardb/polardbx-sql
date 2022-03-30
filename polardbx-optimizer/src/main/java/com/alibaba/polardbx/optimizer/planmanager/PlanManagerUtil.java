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

package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.ast.TDDLHint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.planmanager.feedback.PhyFeedBack;
import com.alibaba.polardbx.optimizer.sharding.ConditionExtractor;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.label.PredicateNode;
import com.alibaba.polardbx.optimizer.sharding.result.ExtractionResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.common.model.SqlType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.ast.TDDLHint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.schema.MetaDbSchema;
import com.alibaba.polardbx.optimizer.config.schema.MysqlSchema;
import com.alibaba.polardbx.optimizer.config.schema.PerformanceSchema;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlaceHolderExecutionPlan;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.CollectTableNameVisitor;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.planmanager.parametric.Point;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.meta.ShardFunctionMeta;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.TDDLSqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static com.alibaba.polardbx.optimizer.planmanager.PlanManager.MAX_TOLERANCE_RATIO;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManager.MINOR_TOLERANCE_RATIO;
import static com.alibaba.polardbx.optimizer.planmanager.parametric.MyParametricQueryAdvisor.INFLATION_NARROW_MAX;
import static com.alibaba.polardbx.optimizer.planmanager.parametric.MyParametricQueryAdvisor.STEADY_CHOOSE_TIME;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainAdvisor;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainOptimizer;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainSharding;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainStatistics;
import static org.apache.calcite.sql.SqlKind.STATISTICS_AFFINITY;

public class PlanManagerUtil {
    private static final Logger logger = LoggerFactory.getLogger(PlanManagerUtil.class);

    public static final Logger loggerSpm = LoggerFactory.getLogger("spm");

    public static void applyCache(RelNode post) {
        /**
         * apply cache
         */
        boolean forbidCache = PlannerContext.getPlannerContext(post)
            .getParamManager()
            .getBoolean(ConnectionParams.FORBID_APPLY_CACHE);
        if (!forbidCache) {
            List<RelNode> cacheNodes = RelUtils.findCacheNodes(post);
            PlannerContext.getPlannerContext(post).setApply(true).getCacheNodes().addAll(cacheNodes);
        }
    }

    public static String relNodeToJson(RelNode plan) {
        return relNodeToJson(plan, false);
    }

    public static String relNodeToJson(RelNode plan, boolean supportMpp) {
        DRDSRelJsonWriter drdsRelJsonWriter = new DRDSRelJsonWriter(supportMpp);
        plan.explain(drdsRelJsonWriter);
        String serialPlan = drdsRelJsonWriter.asString();

        return serialPlan;
    }

    public static RelNode jsonToRelNode(String json, RelOptCluster cluster, RelOptSchema relOptSchema) {
        return jsonToRelNode(json, cluster, relOptSchema, false);
    }

    public static RelNode jsonToRelNode(String json, RelOptCluster cluster, RelOptSchema relOptSchema,
                                        boolean supportMpp) {
        DRDSRelJsonReader drdsRelJsonReader = new DRDSRelJsonReader(cluster, relOptSchema, null, supportMpp);
        try {
            return drdsRelJsonReader.read(json);
        } catch (IOException e) {
            throw new IllegalArgumentException("internalize plan error: " + json, e);
        }
    }

    public static Map<LogicalTableScan, RexNode> getRexNodeTableMap(RelNode rel) {
        /**
         * parameter -> cardinality
         */
        final ExtractionResult er;
        try {
            er = ConditionExtractor.partitioningConditionFrom(rel).extract();
        } catch (Exception e) {
            return Maps.newHashMap();
        }

        Map<LogicalTableScan, RexNode> cardinalityMap = Maps.newConcurrentMap();
        for (Map.Entry<RelNode, Label> entry : er.getRelLabelMap().entrySet()) {
            if (entry.getKey() instanceof LogicalTableScan && entry.getValue().getPushdown() != null) {
                cardinalityMap.put((LogicalTableScan) entry.getKey(),
                    buildRexNodeFromPredicateNode(rel.getCluster().getRexBuilder(), entry.getValue().getPushdown()));
            }
        }
        return cardinalityMap;
    }

    private static RexNode buildRexNodeFromPredicateNode(RexBuilder rexBuilder, PredicateNode predicateNode) {
        if (predicateNode == null || predicateNode.getPredicates().size() == 0) {
            return null;
        }
        if (predicateNode.getPredicates().size() == 1) {
            return predicateNode.getPredicates().get(0);
        }
        return rexBuilder.makeCall(SqlStdOperatorTable.AND, predicateNode.getPredicates());
    }

    public static byte[] compressPlan(String serialPlan) {
        byte[] compressValue;
        try {
            compressValue = compress(serialPlan.getBytes());
        } catch (IOException e) {
            throw new AssertionError("impossible");
        }
        return compressValue;
    }

    public static String uncompressPlan(byte[] inputByte) {
        return new String(uncompress(inputByte));
    }

    public static byte[] uncompress(byte[] inputByte) {
        int len = 0;
        Inflater infl = new Inflater();
        infl.setInput(inputByte);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] outByte = new byte[1024];
        try {
            while (!infl.finished()) {
                len = infl.inflate(outByte);
                if (len == 0) {
                    break;
                }
                bos.write(outByte, 0, len);
            }
            infl.end();
        } catch (Exception e) {
            //
        } finally {
            try {
                bos.close();
            } catch (IOException e) {
                logger.warn("close error");
            }
        }
        return bos.toByteArray();
    }

    public static byte[] compress(byte[] inputByte) throws IOException {
        int len = 0;
        Deflater defl = new Deflater();
        defl.setInput(inputByte);
        defl.finish();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] outputByte = new byte[1024];
        try {
            while (!defl.finished()) {
                len = defl.deflate(outputByte);
                bos.write(outputByte, 0, len);
            }
            defl.end();
        } finally {
            bos.close();
        }
        return bos.toByteArray();
    }

    public static boolean useSPM(String schemaName, ExecutionPlan executionPlan, String parameterizedSql,
                                 ExecutionContext executionContext) {
        ParamManager paramManager = executionContext.getParamManager();

        if (!paramManager.getBoolean(ConnectionParams.ENABLE_SPM)) {
            return false;
        }

        if (executionPlan == PlaceHolderExecutionPlan.INSTANCE) {
            return false;
        }

        if (executionContext.getSqlType() == SqlType.GET_INFORMATION_SCHEMA) {
            return false;
        }

        if (parameterizedSql != null && OptimizerContext.getContext(schemaName).getPlanManager().getBaselineMap()
            .containsKey(parameterizedSql)) {
            return true;
        }

        PlannerContext plannerContext = PlannerContext.getPlannerContext(executionPlan.getPlan());

        if (!plannerContext.isNeedSPM()) {
            return false;
        }

        return canConvertToJson(executionPlan, paramManager);
    }

    public static boolean canConvertToJson(ExecutionPlan executionPlan, ParamManager paramManager) {

        if (executionPlan == PlaceHolderExecutionPlan.INSTANCE) {
            return false;
        }

        // do not support select for update
        if (executionPlan.getAst() instanceof TDDLSqlSelect && ((TDDLSqlSelect) executionPlan.getAst()).withLock()) {
            return false;
        }

        // do not support PhyTableOperation Externalization
        if (executionPlan.getPlan() instanceof BaseQueryOperation) {
            return false;
        }

        PlannerContext plannerContext = PlannerContext.getPlannerContext(executionPlan.getPlan());

        if (!paramManager.getBoolean(ConnectionParams.ENABLE_SPM)) {
            return false;
        }
        if (!plannerContext.getSqlKind().belongsTo(SqlKind.QUERY)) {
            return false;
        }

        // if plan contain apply return false
//        if (OptimizerUtils.hasSubquery(executionPlan.getPlan())) {
//            return false;
//        }

        return true;
    }

    public static Set<Pair<String, String>> getTableSetFromAst(SqlNode ast) {
        final Set<Pair<String, String>> schemaTables = new HashSet<>();
        ast.accept(new CollectTableNameVisitor() {
            @Override
            protected SqlNode buildSth(SqlNode sqlNode) {
                if (sqlNode instanceof SqlIdentifier) {
                    String schema = null;
                    if (!((SqlIdentifier) sqlNode).isSimple()) {
                        schema = ((SqlIdentifier) sqlNode).names.get(0);
                    }
                    String table = ((SqlIdentifier) sqlNode).getLastName();
                    schemaTables.add(Pair.of(schema, table));
                }
                return sqlNode;
            }
        });

        // deal with [top] SqlWith
        if (ast instanceof SqlWith) {
            SqlNodeList sqlNodeList = (SqlNodeList) (((SqlWith) ast).getOperandList().get(0));
            for (SqlNode sqlNode : sqlNodeList) {
                if (sqlNode instanceof SqlWithItem) {
                    for (Pair<String, String> p : schemaTables) {
                        if (p.getValue().equals(((SqlWithItem) sqlNode).name.getSimple())) {
                            schemaTables.remove(p);
                            break;
                        }
                    }
                }
            }
        }

        return schemaTables;
    }

    public static Map<String, TableMeta> getTableMetaSetByTableSet(Set<Pair<String, String>> tableSet,
                                                                   ExecutionContext ec) {
        Map<String, TableMeta> tableMetaSet = new TreeMap<>();
        try {
            if (tableSet != null && !tableSet.isEmpty()) {
                tableSet.forEach(p -> {
                    String db = p.getKey();
                    String tb = p.getValue();
                    if (InformationSchema.NAME.equalsIgnoreCase(db)
                        || MysqlSchema.NAME.equalsIgnoreCase(db)
                        || MetaDbSchema.NAME.equalsIgnoreCase(db)) {
                        return;
                    }
                    OptimizerContext oc = OptimizerContext.getContext(db);
                    String schemaName = oc.getSchemaName();
                    if (oc != null) {
                        TableMeta tbMeta = ec.getSchemaManager(schemaName).getTable(tb);
                        if (tbMeta != null) {
                            String dbTbKey = String.format("%s.%s", schemaName, tb);
                            tableMetaSet.putIfAbsent(dbTbKey, tbMeta);
                        }
                    }
                });
            }
        } catch (Throwable ex) {
            // ignore ex , so make no effects to build plan
        }
        return tableMetaSet;
    }

    public static Set<Pair<String, String>> getTableSetFromPlan(RelNode plan) {
        // FIXME: deal with subquery
        LogicalViewFinder logicalViewFinder = new LogicalViewFinder();
        plan.accept(logicalViewFinder);
        List<LogicalView> logicalViewList = logicalViewFinder.getResult();
        final Set<Pair<String, String>> schemaTables = new HashSet<>();
        for (LogicalView logicalView : logicalViewList) {
            Set<Pair<String, String>> tmpSet = getTableSetFromAst(logicalView.getNativeSqlNode());
            schemaTables.addAll(tmpSet);
        }
        return schemaTables;
    }

    public static int computeTablesHashCode(Set<Pair<String, String>> schemaTables, String defaultSchemaName,
                                            ExecutionContext ec) {
        if (schemaTables == null) {
            return PlanManager.ERROR_TABLES_HASH_CODE;
        }
        /** use associatity property of interger addition to calculate tables hashcode,
         * so we can use HashSet ignoring order */
        HashCombiner hash = new HashCombiner();
        for (Pair<String, String> pair : schemaTables) {
            String schema = (pair.getKey() != null ? pair.getKey() : defaultSchemaName);
            String table = pair.getValue();
            if (InformationSchema.NAME.equalsIgnoreCase(schema)
                || PerformanceSchema.NAME.equalsIgnoreCase(schema)
                || MysqlSchema.NAME.equalsIgnoreCase(schema)
                || MetaDbSchema.NAME.equalsIgnoreCase(schema)) {
                return schema.hashCode();
            }
            try {

                TableMeta tableMeta;
                if (ec == null) {
                    tableMeta = OptimizerContext.getContext(schema).getLatestSchemaManager().getTable(table);
                } else {
                    tableMeta = ec.getSchemaManager(schema).getTable(table);
                }

                // table name
                hash.append(tableMeta.getTableName());
                // all column
                for (ColumnMeta columnMeta : tableMeta.getAllColumns()) {
                    hash.append(columnMeta);
                }
                // secondary index
                for (IndexMeta indexMeta : tableMeta.getSecondaryIndexes()) {
                    for (ColumnMeta columnMeta : indexMeta.getKeyColumns()) {
                        hash.append(columnMeta);
                    }
                }
                // primary key
                if (tableMeta.isHasPrimaryKey()) {
                    for (ColumnMeta columnMeta : tableMeta.getPrimaryKey()) {
                        hash.append(columnMeta);
                    }
                }
                // unique key
                for (IndexMeta indexMeta : tableMeta.getUniqueIndexes(false)) {
                    for (ColumnMeta columnMeta : indexMeta.getKeyColumns()) {
                        hash.append(columnMeta);
                    }
                }
                // global secondary index
                if (tableMeta.getGsiTableMetaBean() != null) {
                    hash.append(tableMeta.getGsiTableMetaBean());
                }

                // scale out status
                if (tableMeta.getComplexTaskTableMetaBean() != null) {
                    hash.append(tableMeta.getComplexTaskTableMetaBean());
                }

                if (tableMeta.getTableGroupOutlineRecord() != null) {
                    hash.append(tableMeta.getTableGroupOutlineRecord());
                }

                // curr partition info
                if (tableMeta.getPartitionInfo() != null) {
                    hash.append(tableMeta.getPartitionInfo());
                }

                // new partition info
                if (tableMeta.getNewPartitionInfo() != null) {
                    hash.append(tableMeta.getNewPartitionInfo());
                }

                // Table flag status.
                if (tableMeta.isAutoPartition()) {
                    hash.append(0xA55A); // Magic number.
                }

                TableRule tableRule = OptimizerContext.getContext(schema).getRuleManager().getTableRule(table);
                if (tableRule != null) {
                    // tableRule
                    hash.append(tableRule.isBroadcast());
                    hash.append(tableRule.isAllowFullTableScan());

                    hash.append(tableRule.getDbNamePattern());
                    hash.append(tableRule.getTbNamePattern());

                    String[] dbRuleStrs = tableRule.getDbRuleStrs();
                    if (dbRuleStrs != null) {
                        for (String s : dbRuleStrs) {
                            hash.append(s);
                        }
                    }

                    String[] tbRuleStrs = tableRule.getTbRulesStrs();
                    if (tbRuleStrs != null) {
                        for (String s : tbRuleStrs) {
                            hash.append(s);
                        }
                    }

                    ShardFunctionMeta dbShardFuncTionMeta = tableRule.getDbShardFunctionMeta();
                    if (dbShardFuncTionMeta != null) {
                        hash.append(dbShardFuncTionMeta.getRuleShardFuncionName());
                    }

                    ShardFunctionMeta tbShardFuncTionMeta = tableRule.getTbShardFunctionMeta();
                    if (tbShardFuncTionMeta != null) {
                        hash.append(tbShardFuncTionMeta.getRuleShardFuncionName());
                    }

                    List<MappingRule> extPartitions = tableRule.getExtPartitions();
                    if (extPartitions != null) {
                        hash.append(extPartitions);
                    }
                }

            } catch (Throwable e) {
                loggerSpm.debug("plan manager compute tables hash code error", e);
                return PlanManager.ERROR_TABLES_HASH_CODE;
            }
        }
        return hash.result();
    }

    /**
     * Combine hash code of multiple objects
     *
     * @see java.util.Objects#hash(Object...)
     */
    public static class HashCombiner {
        private int hashCode = 1;

        void append(Object element) {
            hashCode = hashCode + (element == null ? 0 : element.hashCode());
        }

        int result() {
            return hashCode;
        }
    }

    public static String getPlanOrigin(RelNode plan) {
        if (PlannerContext.getPlannerContext(plan).getWorkloadType() == WorkloadType.AP) {
            return "AP";
        } else {
            return "TP";
        }
    }

    public static WorkloadType getWorkloadType(PlanInfo planInfo) {
        if (planInfo.getOrigin() != null) {
            if (planInfo.getOrigin().equalsIgnoreCase("AP") ||
                planInfo.getOrigin().equalsIgnoreCase("SMP_AP") ||
                planInfo.getOrigin().equalsIgnoreCase("MPP")) {
                return WorkloadType.AP;
            } else {
                return WorkloadType.TP;
            }
        }
        return null;
    }

    public static boolean cacheSqlKind(SqlKind kind) {
        switch (kind) {
        case SELECT:
        case UNION:
        case INTERSECT:
        case EXCEPT:
        case ORDER_BY:
        case INSERT:
        case REPLACE:
        case UPDATE:
        case DELETE:
        case WITH:
            return true;
        default:
            return false;
        }
    }

    public static boolean useSpm(SqlParameterized sqlParameterized, ExecutionContext ec) {
        if (ec.getSqlType() == SqlType.GET_SYSTEM_VARIABLE) {
            return false;
        }
        ExplainResult explain = ec.getExplain();
        if (isExplainOptimizer(explain)) {
            return false;
        }

        if (isExplainSharding(explain)) {
            return false;
        }

        if (isExplainAdvisor(explain)) {
            return false;
        }

        if (isExplainStatistics(explain)) {
            return false;
        }
        if (!sqlParameterized.needCache()) {
            return false;
        }

        long maxCacheParams = ec.getParamManager().getLong(ConnectionParams.MAX_CACHE_PARAMS);
        if (sqlParameterized.getParameters().size() > maxCacheParams) {
            return false;
        }

        if (ConfigDataMode.isFastMock()) {
            return false;
        }

        if (ec.getLoadDataContext() != null) {
            return false;
        }

        /**
         * hint judgement, sql with hint should avoid get into spm
         */

        if (sqlParameterized.getAst().getHeadHintsDirect() != null) {
            if (sqlParameterized.getAst().getHeadHintsDirect().stream()
                .anyMatch(sqlCommentHint -> sqlCommentHint instanceof TDDLHint)) {
                return false;
            }
        }

        if ((sqlParameterized.getAst() instanceof SQLObjectImpl)
            && ((SQLObjectImpl) (sqlParameterized.getAst())).getHint() != null) {
            if (((SQLObjectImpl) (sqlParameterized.getAst())).getHint() instanceof TDDLHint) {
                return false;
            }
        }

        /**
         * sql type judgement, only support SELECT/INSERT/UPDATE/DELETE get into plancache
         */
        if (
            !(sqlParameterized.getAst() instanceof SQLSelectStatement ||
                sqlParameterized.getAst() instanceof SQLInsertStatement ||
                sqlParameterized.getAst() instanceof SQLUpdateStatement ||
                sqlParameterized.getAst() instanceof SQLDeleteStatement)) {
            return false;
        }

        return ec.getParamManager().getBoolean(ConnectionParams.PLAN_CACHE);
    }

    public static boolean isTolerated(double expectedRowCount, double actuallyRowCount, int minorValue) {

        if (expectedRowCount == -1) {
            return true;
        }
        expectedRowCount += 1;
        actuallyRowCount += 1;
        double differ = Math.abs(expectedRowCount - actuallyRowCount);
        if (differ < minorValue || ((actuallyRowCount / expectedRowCount) > MINOR_TOLERANCE_RATIO
            && (actuallyRowCount / expectedRowCount) < MAX_TOLERANCE_RATIO)) {
            return true;
        }
        return false;
    }

    public static boolean isSimpleCondition(RexNode condition) {
        if (condition == null) {
            return false;
        }

        if (!(condition instanceof RexCall)) {
            return false;
        }
        final RexCall currentCondition = (RexCall) condition;
        if (currentCondition.getKind().belongsTo(STATISTICS_AFFINITY)) {
            return true;
        }
        return false;
    }

    public static String pointsToJson(Collection<Point> points) {
        if (points == null) {
            return "";
        }
        final JsonBuilder jsonBuilder = new JsonBuilder();
        List<Map<String, Object>> abstractPointList = Lists.newArrayList();
        for (Point point : points) {
            Map<String, Object> abstractPoint = Maps.newHashMap();
            abstractPoint.put("selectivity", jsonBuilder.toJsonString(point.getSelectivityMap()));
            abstractPoint.put("params", jsonBuilder.toJsonString(point.getParams()));
            abstractPoint.put("planId", jsonBuilder.toJsonString(point.getPlanId()));
            abstractPoint.put("chooseTime", jsonBuilder.toJsonString(point.getChooseTime()));
            abstractPoint.put("rowcountExpected", jsonBuilder.toJsonString(point.getRowcountExpected()));
            abstractPoint.put("maxRowcountExpected", jsonBuilder.toJsonString(point.getMaxRowcountExpected()));
            abstractPoint.put("minRowcountExpected", jsonBuilder.toJsonString(point.getMinRowcountExpected()));
            abstractPoint
                .put("xRowcountExpected", jsonBuilder.toJsonString(point.getPhyFeedBack().getExaminedRowCount()));
            abstractPointList.add(abstractPoint);
        }
        return jsonBuilder.toJsonString(abstractPointList);
    }

    public static Set<Point> jsonToPoints(String parameterSql,
                                          List<Map<String, Object>> abstractPointList) {
        Set<Point> points = Sets.newHashSet();
        for (Map<String, Object> pointMap : abstractPointList) {
            Map<String, BigDecimal> selectivityMap =
                (Map<String, BigDecimal>) com.alibaba.fastjson.JSON.parse((String) pointMap.get("selectivity"));
            List<Object> params =
                (List<Object>) com.alibaba.fastjson.JSON.parse((String) pointMap.get("params"));
            int planId = (int) com.alibaba.fastjson.JSON.parse((String) pointMap.get("planId"));
            long chooseTime = Long.valueOf((String) pointMap.get("chooseTime"));
            double rowcountExpected = Double.valueOf(
                pointMap.get("rowcountExpected") == null ? -1 + "" : (String) pointMap.get("rowcountExpected"));
            double maxRowcountExpected = Double.valueOf(pointMap.get("maxRowcountExpected") == null ? -1 + "" :
                (String) pointMap.get("maxRowcountExpected"));
            double minRowcountExpected = Double.valueOf(
                pointMap.get("minRowcountExpected") == null ? -1 + "" : (String) pointMap.get("minRowcountExpected"));
            long xRowcountExpected = Long.valueOf(
                pointMap.get("xRowcountExpected") == null ? -1 + "" : (String) pointMap.get("xRowcountExpected"));

            Map<String, Double> selectivityDoubleMap = Maps.newTreeMap();
            for (Map.Entry<String, BigDecimal> entry : selectivityMap.entrySet()) {
                selectivityDoubleMap.put(entry.getKey(), entry.getValue().doubleValue());
            }
            Point point = new Point(parameterSql, selectivityDoubleMap, params, planId, rowcountExpected, chooseTime,
                maxRowcountExpected, minRowcountExpected, new PhyFeedBack(xRowcountExpected, null));
            points.add(point);
        }
        return points;
    }

    public static long getBaselineIdFromExecutionContext(ExecutionContext executionContext) {
        if (executionContext.getFinalPlan() == null) {
            return -1;
        }
        if (executionContext.getFinalPlan().getPlan() == null) {
            return -1;
        }
        PlannerContext plannerContext = PlannerContext.getPlannerContext(executionContext.getFinalPlan().getPlan());
        if (plannerContext == null) {
            return -1;
        }

        if (plannerContext.getBaselineInfo() == null) {
            return -1;
        }

        return plannerContext.getBaselineInfo().getId();
    }

    public static void mergeColumns(Map<String, Set<String>> columnsMap,
                                    Map<String, Set<String>> columnsMapTmp) {
        for (Map.Entry<String, Set<String>> entry : columnsMapTmp.entrySet()) {
            String tableName = entry.getKey();
            Set<String> columns = entry.getValue();

            if (columnsMap.containsKey(tableName)) {
                columnsMap.get(tableName).addAll(columns);
            } else {
                columnsMap.put(tableName, columns);
            }
        }
    }

    /**
     * get columns involved from plan
     *
     * @return table name -> column name collection
     */
    public static Map<String, Set<String>> getColumnsFromPlan(String schema, RelNode plan) {
        Map<String, Set<String>> columnsMap = Maps.newHashMap();
        Map<LogicalTableScan, RexNode> rexNodeMap = PlanManagerUtil.getRexNodeTableMap(plan);
        if (rexNodeMap == null) {
            return columnsMap;
        }
        for (Map.Entry<LogicalTableScan, RexNode> entry : rexNodeMap.entrySet()) {
            RelOptTable table = entry.getKey().getTable();
            TableMeta tableMeta = (TableMeta) ((RelOptTableImpl) table).getImplTable();

            String tableName = tableMeta.getTableName();
            String schemaName = tableMeta.getSchemaName();

            if (schema != null && !schema.equals(schemaName)) {
                continue;
            }
            if (SystemDbHelper.isDBBuildIn(schemaName)) {
                continue;
            }

            List<String> fieldNames = table.getRowType().getFieldNames();
            RexNode rex = entry.getValue();

            /**
             * rexnode to column name
             */
            ImmutableBitSet bitSet = RelOptUtil.InputFinder.bits(rex);
            for (Integer fieldIndex : bitSet) {
                String columnName = fieldNames.get(fieldIndex);
                // TODO try to find the statistic need of multi column by analyzing plan
                columnsMap.put(tableName, Sets.newHashSet(columnName));
            }
        }
        return columnsMap;
    }
}
