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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.Xplan.XPlanTemplate;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.util.DynamicParamInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.IndexedDynamicParamInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.RuntimeFilterDynamicParamInfo;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.convertor.ConvertorHelper;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
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
import org.apache.commons.lang3.StringUtils;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.sql.SqlKind.PLUS;

/**
 * 构建 PhyTableOperation, 做 UNION 优化
 * <p>
 * TODO: UNION 优化需要根据 HINT 以及其他参数控制
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

    public PhyTableScanBuilder(SqlSelect sqlTemplate, Map<String, List<List<String>>> targetTables,
                               ExecutionContext executionContext, RelNode parent, DbType dbType,
                               RelDataType rowType, String schemaName, List<String> logicalTableNames) {
        this.executionContext = executionContext;
        this.targetTables = targetTables;
        this.params = executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter();
        this.parent = parent;
        this.dbType = dbType;
        this.rowType = rowType;

        boolean usingPhySqlCache = executionContext.enablePhySqlCache();
        if (usingPhySqlCache &&
            (executionContext.getCorrelateFieldInViewMap() == null || executionContext
                .getCorrelateFieldInViewMap().isEmpty())) {
            this.sqlTemplate = sqlTemplate;
            this.sqlTemplate.accept(new FetchPreprocessor(params, true));
        } else {
            this.sqlTemplate = (SqlSelect) sqlTemplate.accept(
                new ReplaceTableNameWithSomethingVisitor(executionContext.getCorrelateFieldInViewMap(), schemaName,
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
            logicalTableName);
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

        List<RelNode> phyTableScans = new ArrayList<>();
        CursorMeta cursorMeta = CursorMeta.build(CalciteUtils.buildColumnMeta(rowType, "TableScan"));

        String sqlTemplateStr;
        if (parent instanceof LogicalView
            && ((LogicalView) parent).getSqlTemplate() == sqlTemplate) {
            sqlTemplateStr = ((LogicalView) parent).getSqlTemplateStr();
        } else {
            sqlTemplateStr = RelUtils.toNativeSql(sqlTemplate, dbType);
        }

        ByteString sqlTemplateDigest = null;
        // Init sql digest.
        try {
            sqlTemplateDigest = com.google.protobuf.ByteString
                .copyFrom(MessageDigest.getInstance("md5").digest(sqlTemplateStr.getBytes()));
        } catch (Exception ignore) {
        }

        ShardPlanMemoryContext shardPlanMemoryContext = buildShardPlanMemoryContext(parent,
            sqlTemplateStr,
            (AbstractRelNode) parent,
            this.params,
            this.targetTables,
            this.executionContext);
        MemoryAllocatorCtx maOfPlanBuildingPool = shardPlanMemoryContext.memoryAllocator;
        long phyOpMemSize = shardPlanMemoryContext.phyOpMemSize;
        final XPlanTemplate XPlan = parent instanceof LogicalView ? ((LogicalView) parent).getXPlan() : null;
        for (Map.Entry<String, List<List<String>>> t : targetTables.entrySet()) {
            String group = t.getKey();
            List<List<String>> tableNames = t.getValue();
            int realUnionSize =
                unionOptHelper != null ? unionOptHelper.calMergeUnionSize(tableNames.size(), group) : unionSize;
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
                    sqlTemplateStr,
                    1,
                    rowType,
                    maOfPlanBuildingPool);
                phyTableOp.setLogicalTableNames(logicalTableNames);
                phyTableOp.setXTemplate(XPlan);
                phyTableOp.setSqlDigest(sqlTemplateDigest);
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
                        sqlTemplateStr,
                        realUnionSize,
                        rowType,
                        maOfPlanBuildingPool);
                    phyTableOp.setLogicalTableNames(logicalTableNames);
                    phyTableOp.setXTemplate(XPlan);
                    phyTableOp.setSqlDigest(sqlTemplateDigest);
                    phyTableScans.add(phyTableOp);
                    i = endIndex;
                }
            }
        }
        return phyTableScans;
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

//    public List<ParameterContext> buildParams(PhyTableOperation phyTableOp) {
//        Preconditions.checkArgument(CollectionUtils.isNotEmpty(phyTableOp.getTableNames()));
//
//        List<ParameterContext> params = new ArrayList<>();
//        for (List<String> ts : phyTableOp.getTableNames()) {
//            Preconditions.checkArgument(CollectionUtils.isNotEmpty(ts));
//
//            int tableIndex = -1;
//            for (int i : paramIndex) {
//                if (i == TABLE_NAME_PARAM_INDEX) {
//                    tableIndex += 1;
//                    params.add(buildParameterContextForTableName(ts.get(tableIndex), 0));
//                } else if (i == SCALAR_SUBQUERY_PARAM_INDEX) {
//                    // do nothing
//                } else if (i == APPLY_SUBQUERY_PARAM_INDEX) {
//                    // do nothing
//                } else {
//                    params.add(this.params.get(i + 1));
//                }
//            }
//        }
//        return params;
//    }
//
//    public String buildSql(PhyTableOperation phyTableOp, String prefix) {
//        return RelUtils.unionSql(phyTableOp.getTableNames().size(),
//            phyTableOp.getNativeSql(),
//            sqlTemplate,
//            dbType,
//            prefix);
//    }

    /**
     * 构建 SQL 对应的参数信息
     */
//    protected Map<Integer, ParameterContext> buildParams(List<List<String>> tableNames) {
//        Preconditions.checkArgument(CollectionUtils.isNotEmpty(tableNames));
//
//        int index = 1;
//        Map<Integer, ParameterContext> params = new HashMap<>();
//        for (List<String> ts : tableNames) {
//            index = buildParam(params, index, ts);
//        }
//        return params;
//    }
    public List<ParameterContext> buildParams(List<List<String>> tableNames) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(tableNames));

        List<ParameterContext> results = new ArrayList<>();
        for (List<String> ts : tableNames) {
            Preconditions.checkArgument(CollectionUtils.isNotEmpty(ts));
            results.addAll(buildSplitParams(ts));
        }
        return results;
    }

    public List<ParameterContext> buildSplitParams(List<String> tables) {
        List<ParameterContext> results = new ArrayList<>();
        int tableIndex = -1;
        for (DynamicParamInfo dynamicParamInfo : dynamicParamList) {
            if (dynamicParamInfo instanceof IndexedDynamicParamInfo) {
                int i = ((IndexedDynamicParamInfo) dynamicParamInfo).getParamIndex();
                if (i == PlannerUtils.TABLE_NAME_PARAM_INDEX) {
                    tableIndex += 1;
                    results.add(PlannerUtils.buildParameterContextForTableName(tables.get(tableIndex), 0));
                } else if (i == PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX) {
                    // do nothing
                } else if (i == PlannerUtils.APPLY_SUBQUERY_PARAM_INDEX) {
                    // do nothing
                } else {
                    results.add(this.params.get(i + 1));
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
//    /**
//     * Build parameters of NativeSql
//     */
//    protected int buildParam(Map<Integer, ParameterContext> params, int index, List<String> tableNames) {
//        Preconditions.checkArgument(CollectionUtils.isNotEmpty(tableNames));
//
//        int tableIndex = -1;
//        for (int i : paramIndex) {
//            if (i == TABLE_NAME_PARAM_INDEX) {
//
//                tableIndex += 1;
//                params.put(index, buildParameterContextForTableName(tableNames.get(tableIndex), index));
//                index++;
//
//            } else if (i == SCALAR_SUBQUERY_PARAM_INDEX) {
//                //do nothing
//            } else if (i == APPLY_SUBQUERY_PARAM_INDEX) {
//                //do nothing
//            } else {
//                params.put(index, changeParameterContextIndex(this.params.get(i + 1), index));
//                index++;
//            }
//        }
//        return index;
//    }

    public void setUnionSize(int unionSize) {
        this.unionSize = unionSize;
    }

    public void setUnionOptHelper(UnionOptHelper unionOptHelper) {
        this.unionOptHelper = unionOptHelper;
    }

    protected PhyTableOperation buildOnePhyTableOperatorForScan(String group, List<List<String>> tableNames,
                                                                CursorMeta cursorMeta, String sqlTemplateStr,
                                                                int realUnionSize, RelDataType rowTyppe,
                                                                MemoryAllocatorCtx maOfPlanBuildingPool) {
        PhyTableOperation phyTableOp =
            new PhyTableOperation(parent.getCluster(), parent.getTraitSet(), parent.getRowType(), cursorMeta, parent);
        phyTableOp.setDbIndex(group);
        phyTableOp.setTableNames(tableNames);
        phyTableOp.setNativeSqlNode(sqlTemplate);
        phyTableOp.setDbType(dbType);
        phyTableOp.setSchemaName(schemaName);
        phyTableOp.setLockMode(sqlTemplate.getLockMode());
        phyTableOp.setSqlTemplate(sqlTemplateStr);
        phyTableOp.setRowType(rowTyppe);
        phyTableOp.setPhyOperationBuilder(this);
        phyTableOp.setUnionSize(realUnionSize);
        phyTableOp.setMemoryAllocator(maOfPlanBuildingPool);

        return phyTableOp;
    }

    public Map<Integer, ParameterContext> getParams() {
        return params;
    }

    public final static String UNION_KW = "\nUNION ALL\n";
    public final static String ORDERBY_KW = " ORDER BY ";
    public final static String LIMIT_KW = " LIMIT ";
    public final static String UNION_ALIAS = "__DRDS_ALIAS_T_";

    public String buildSql(PhyTableOperation phyTableOp, String prefix) {
        int tableCount = phyTableOp.getTableNames().size();
        String orderBy = buildPhysicalOrderByClause();
        return buildPhysicalQuery(tableCount, phyTableOp.getNativeSql(), orderBy, prefix, -1);
    }

    /**
     * Use union all to reduce the amount of physical sql.
     *
     * @param num number of sub-queries
     */
    public static String buildPhysicalQuery(int num, String sqlTemplateStr, String orderBy, String prefix, long limit) {
        Preconditions.checkArgument(num > 0, "The number of tables must great than 0 when build UNION ALL sql");
        if (num == 1) {
            if (StringUtils.isNotEmpty(prefix)) {
                return prefix + sqlTemplateStr;
            } else {
                return sqlTemplateStr;
            }
        }

        StringBuilder builder = new StringBuilder();
        if (prefix != null) {
            builder.append(prefix);
        }
        if (orderBy != null) {
            builder.append("SELECT * FROM (");
        }

        builder.append("( ").append(sqlTemplateStr).append(" )");
        for (int i = 1; i < num; i++) {
            builder.append(UNION_KW).append("( ").append(sqlTemplateStr).append(") ");
        }

        // 最终生成的 UNION ALL SQL,需要在最外层添加 OrderBy
        // 不能添加limit 和 offset, 有聚合函数的情况下会导致结果错误
        if (orderBy != null) {
            builder.append(") ").append(UNION_ALIAS).append(" ").append(ORDERBY_KW).append(orderBy);
        }

        if (limit > 0) {
            builder.append(LIMIT_KW).append(limit);
        }
        return builder.toString();
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
}
