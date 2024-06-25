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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByHash;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.gms.metadb.seq.SequencesRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.lbac.LBACSecurityLabel;
import com.alibaba.polardbx.gms.lbac.LBACSecurityPolicy;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.locality.LocalityInfo;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.newrule.DateRuleGen;
import com.alibaba.polardbx.optimizer.utils.newrule.IRuleGen;
import com.alibaba.polardbx.optimizer.utils.newrule.RuleUtils;
import com.alibaba.polardbx.optimizer.view.InformationSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.MysqlSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.PerformanceSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.Rule;
import com.alibaba.polardbx.rule.Rule.RuleColumn;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.ddl.PartitionByType;
import com.alibaba.polardbx.rule.ddl.PartitionByTypeUtils;
import com.alibaba.polardbx.rule.impl.WrappedGroovyRule;
import com.alibaba.polardbx.rule.meta.ShardFunctionMeta;
import com.alibaba.polardbx.rule.model.AdvancedParameter;
import com.alibaba.polardbx.rule.model.DateEnumerationParameter;
import com.alibaba.polardbx.rule.utils.GroovyRuleTimeShardFuncionUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.Math.max;

/**
 * @author chenmo.cm
 */
public class LogicalShowCreateTablesForShardingDatabaseHandler extends HandlerCommon {

    private static final Logger logger =
        LoggerFactory.getLogger(LogicalShowCreateTablesForShardingDatabaseHandler.class);

    public LogicalShowCreateTablesForShardingDatabaseHandler(IRepository repo) {
        super(repo);
    }

    private static String getShardKey(List<Rule<String>> rules) throws UnsupportedOperationException {
        if (rules == null) {
            return null;
        }
        List<String> res = new ArrayList<String>();
        for (Rule<String> ruleList : rules) {
            Set<Rule.RuleColumn> tr = ruleList.getRuleColumnSet();
            for (Rule.RuleColumn rc : tr) {
                res.add(rc.key);
            }
        }
        if (res.size() != 1) {
            return null;
        }
        return res.get(0);
    }

    /**
     * @param dbOrTb true for database and false for table
     */
    private static String getShardFunction(String schemaName, String column, List<Object> rules, long tbCountPerGroup,
                                           long dbCount,
                                           boolean useStandAlone, boolean dbOrTb) throws UnsupportedOperationException {
        if (column == null) {
            throw new UnsupportedOperationException("Not supported column");
        }
        if (rules == null) {
            throw new UnsupportedOperationException("not supported rule");
        }
        if (rules.isEmpty() || rules.size() != 1) {
            throw new UnsupportedOperationException("Not supported form");
        }

        Object o = rules.get(0);
        if (!(o instanceof WrappedGroovyRule)) {
            throw new UnsupportedOperationException("Not supported 'if-else' or other rules ");
        }

        WrappedGroovyRule rule = (WrappedGroovyRule) o;
        Map<String, RuleColumn> ruleColumnMap = rule.getRuleColumns();

        if (ruleColumnMap.isEmpty() || ruleColumnMap.size() != 1) {
            throw new UnsupportedOperationException("Not supported form");
        }

        if (!ruleColumnMap.containsKey(column)) {
            throw new UnsupportedOperationException("Not supported form");
        }
        RuleColumn ruleColumn = ruleColumnMap.get(column);
        if (!(ruleColumn instanceof AdvancedParameter)) {
            return "hash";
        }

        AdvancedParameter.AtomIncreaseType type = ((AdvancedParameter) ruleColumn).atomicIncreateType;
        if (type.isTime() || type.isNoloopTime()) {

            DateRuleGen drg = new DateRuleGen(schemaName);
            PartitionByType byType = null;
            Comparable increateValue = ((AdvancedParameter) ruleColumn).atomicIncreateValue;
            int start = 1;
            if (increateValue instanceof DateEnumerationParameter) {
                start = ((DateEnumerationParameter) increateValue).atomicIncreatementNumber;
            }

            for (PartitionByType partitionByType : PartitionByTypeUtils.getAllTimePartitionByTypeList()) {

                StringBuilder builder = new StringBuilder();
                drg.genMiddleRuleStr(partitionByType,
                    column,
                    start,
                    builder,
                    tbCountPerGroup,
                    dbCount,
                    dbOrTb,
                    ((AdvancedParameter) ruleColumn).rangeArray[0].start,
                    ((AdvancedParameter) ruleColumn).rangeArray[0].end);

                String part = builder.toString();
                if (rule.getExpression().contains(part)) {
                    byType = partitionByType;
                    break;
                }
            }

            if (byType == null) {
                throw new UnsupportedOperationException("Not supported yet");
            }
            switch (byType) {
            case MM:
                return "MM";
            case DD:
                return "DD";
            case MMDD:
                return "MMDD";
            case WEEK:
                return "WEEK";
            case YYYYMM:
                return "YYYYMM";
            case YYYYMM_OPT:
                return "YYYYMM_OPT";
            case YYYYDD:
                return "YYYYDD";
            case YYYYDD_OPT:
                return "YYYYDD_OPT";
            case YYYYWEEK:
                return "YYYYWEEK";
            case YYYYWEEK_OPT:
                return "YYYYWEEK_OPT";
            case YYYYMM_NOLOOP:
                return "YYYYMM_NOLOOP";
            case YYYYDD_NOLOOP:
                return "YYYYDD_NOLOOP";
            case YYYYWEEK_NOLOOP:
                return "YYYYWEEK_NOLOOP";
            default:
                throw new UnsupportedOperationException("NOT supported yet");
            }

        }
        return "hash";
    }

    /**
     * #getShardFunction(String,List<Object>,long,long,boolean,boolean)} instead
     */

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowCreateTable showCreateTable = (SqlShowCreateTable) show.getNativeSqlNode();
        final String tableName = RelUtils.lastStringValue(showCreateTable.getTableName());
        String schemaName = show.getSchemaName();
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }
        TableMeta tableMeta = null;

        Cursor cursor = null;
        try {
            ArrayResultCursor result = new ArrayResultCursor("Create Table");

            try {
                boolean isSystemDb = InformationSchema.NAME.equalsIgnoreCase(schemaName) || RelUtils
                    .informationSchema(showCreateTable.getTableName())
                    || RelUtils.mysqlSchema(showCreateTable.getTableName()) || RelUtils
                    .performanceSchema(showCreateTable.getTableName());
                if (isSystemDb) {
                    cursor = showViews(logicalPlan, schemaName, tableName, executionContext);
                    if (cursor != null) {
                        return cursor;
                    }
                }
                tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager()
                    .getTable(tableName);
            } catch (Throwable t) {
                cursor = showViews(logicalPlan, schemaName, tableName, executionContext);
                if (cursor != null) {
                    return cursor;
                }
            }

            SqlNode logTbSqlNode = showCreateTable.getTableName();
            SqlIdentifier phyTbSqlId = null;
            if (logTbSqlNode instanceof SqlIdentifier) {
                SqlIdentifier logTbSqlId = (SqlIdentifier) logTbSqlNode;
                phyTbSqlId = new SqlIdentifier(logTbSqlId.getLastName(), logTbSqlId.getCollation(),
                    logTbSqlId.getParserPosition());
            }

            cursor = repo.getCursorFactory()
                .repoCursor(executionContext,
                    new PhyShow(show.getCluster(),
                        show.getTraitSet(),
                        SqlShowCreateTable.create(SqlParserPos.ZERO,
                            TStringUtil.isEmpty(show.getPhyTable()) ? phyTbSqlId :
                                show.getPhyTableNode()),
                        show.getRowType(),
                        show.getDbIndex(),
                        show.getPhyTable(),
                        show.getSchemaName()));
            Row row = null;
            if ((row = cursor.next()) != null) {
                for (ColumnMeta cm : show.getCursorMeta().getColumns()) {
                    result.addColumn(cm);
                }
                result.initMeta();

                StringBuilder appender = new StringBuilder("");
                PartitionInfo partInfo = null;
                TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
                TableRule tableRule = null;
                tableRule = tddlRuleManager.getTableRule(tableName);

                boolean broadcast = OptimizerContext.getContext(schemaName).getRuleManager().isBroadCast(tableName);
                boolean single = OptimizerContext.getContext(schemaName).getRuleManager().isTableInSingleDb(tableName);
                String defaultGroup = OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(null);
                boolean fail = false;

                long dbCount = -1;
                long tbCountPerGroup = 0;
                ShardFunctionMeta dbShardFuncMeta = null;
                ShardFunctionMeta tbShardFuncMeta = null;
                if (tableRule != null) {
                    tableRule.getActualTopology(); // init actual topology, if
                    // need
                    dbShardFuncMeta = tableRule.getDbShardFunctionMeta();
                    tbShardFuncMeta = tableRule.getTbShardFunctionMeta();
                }
                String dbShardFuncPartitionStr = null;
                String tbShardFuncPartitionStr = null;
                if (tableRule == null || tableRule.getShardColumns().isEmpty()) {
                    // 单表
                    if (broadcast) {
                        appender.append("broadcast");
                    }
                } else if (tableRule.getRuleDbCount() == tableRule.getRuleTbCount()
                    && tableRule.getShardColumns().size() < 2
                    && (tableRule.getPartitionType() == null || !tableRule.getPartitionType().isNoloopTime())) {

                    // 拆分键
                    String dbColumn = tableRule.getShardColumns().get(0);
                    // 拆分个数
                    dbCount = tableRule.getRuleDbCount();
                    // 拆分函数
                    String dbFunction = null;

                    tbCountPerGroup = tableRule.getRuleTbCount() / dbCount;

                    if (dbShardFuncMeta != null) {
                        dbShardFuncPartitionStr = dbShardFuncMeta.buildCreateTablePartitionStr();
                        appender.append(dbShardFuncPartitionStr);
                    } else {

                        try {
                            dbFunction = getShardFunction(schemaName, dbColumn,
                                tableRule.getDbShardRules(),
                                tbCountPerGroup,
                                dbCount,
                                false/* 只有一个拆分键，不可能是useStandAlone=true */,
                                true);

                        } catch (UnsupportedOperationException e) {
                            fail = true;
                        }

                        if (!fail && !SqlValidatorImpl.isImplicitKey(dbColumn)) {
                            // 分库不分表
                            appender.append("dbpartition by ")
                                .append(dbFunction)
                                .append("(`")
                                .append(dbColumn)
                                .append("`)");
                            List<String> genGroups = new ArrayList<String>();
                            RuleUtils.genDBPatitionDefinition(0, 1, OptimizerContext.getContext(schemaName)
                                .getMatrix()
                                .getGroups(), defaultGroup, genGroups);

                            if (dbCount != genGroups.size()) {
                                appender.append(" dbpartitions ").append(dbCount);
                            }
                        }
                    }

                } else {

                    // 分库分表, 或其他
                    Map<String, Set<String>> topo = tableRule.getActualTopology();
                    /**
                     * 获得每个分库上分别有多少个分表
                     */
                    int commonTable = -1;
                    dbCount = 0;
                    for (Map.Entry<String, Set<String>> entry : topo.entrySet()) {
                        // 热点映射分库是常规分库之外的，热点分库上的分表个数可能和其他分库上分表数不相等
                        if (tableRule.getExtPartitions() != null
                            && tableRule.getExtPartitions().size() > 0
                            && tableRule.getExtPartitions()
                            .stream()
                            .anyMatch(i -> i.getDb().equalsIgnoreCase(entry.getKey()))) {
                            continue;
                        }

                        // 计算 dbCount 时，排除热点映射库
                        dbCount++;

                        if (commonTable == -1) {
                            commonTable = entry.getValue().size();
                            continue;
                        }

                        if (commonTable != entry.getValue().size()) {
                            commonTable = -1;
                            break;
                        }

                    }
                    if (commonTable == -1
                        && (tableRule.getPartitionType() == null || !tableRule.getPartitionType().isNoloopTime())) {
                        fail = true;
                    }

                    String dbColumn = null;
                    String tbColumn = null;

                    tbCountPerGroup = commonTable;

                    String dbFunction = null;
                    String tbFunction = null;

                    try {

                        if (tableRule.getDbShardRules() == null) {
                            // 分表不分库
                            dbColumn = null;
                        } else {
                            dbColumn = getShardKey(tableRule.getDbShardRules());
                        }

                        // 如果是分库不分表，tbColumn也可能是null
                        tbColumn = getShardKey(tableRule.getTbShardRules());

                        boolean useStandAlone = false;
                        if (dbColumn != null && tbColumn != null && (!dbColumn.equals(tbColumn))) {
                            useStandAlone = true;
                        }

                        if (dbShardFuncMeta != null) {
                            dbShardFuncPartitionStr = dbShardFuncMeta.buildCreateTablePartitionStr();
                        } else if (GroovyRuleTimeShardFuncionUtils.isGroovyRuleTimeShardFuncion(tableRule,
                            dbColumn,
                            true)) {
                            if (dbColumn != null) {
                                dbShardFuncPartitionStr =
                                    GroovyRuleTimeShardFuncionUtils.buildCreateTablePartitionStr(tableRule,
                                        dbCount,
                                        tbCountPerGroup,
                                        dbColumn,
                                        true);
                            }

                        }

                        if (tbShardFuncMeta != null) {
                            tbShardFuncPartitionStr = tbShardFuncMeta.buildCreateTablePartitionStr();
                        } else if (GroovyRuleTimeShardFuncionUtils.isGroovyRuleTimeShardFuncion(tableRule,
                            tbColumn,
                            false)) {
                            if (tbColumn != null) {
                                tbShardFuncPartitionStr =
                                    GroovyRuleTimeShardFuncionUtils.buildCreateTablePartitionStr(tableRule,
                                        dbCount,
                                        tbCountPerGroup,
                                        tbColumn,
                                        false);
                            }
                        }

                        if (useStandAlone) {

                            if (dbShardFuncPartitionStr == null) {
                                // 当分库键与分表键不一样时， 肯定是使用useStandAlone模式
                                dbFunction = getShardFunction(schemaName, dbColumn,
                                    tableRule.getDbShardRules(),
                                    IRuleGen.TABLE_COUNT_OF_USE_STANT_ALONE,
                                    dbCount,
                                    useStandAlone,
                                    true);
                            }

                            if (tbShardFuncPartitionStr == null) {
                                tbFunction = getShardFunction(schemaName, tbColumn,
                                    tableRule.getTbShardRules(),
                                    tbCountPerGroup,
                                    IRuleGen.GROUP_COUNT_OF_USE_STANT_ALONE,
                                    useStandAlone,
                                    false);
                            }

                        } else {

                            /**
                             * <pre>
                             * 来到这里，有3种可能:
                             *  1. dbColumn != null && tbColumn != null && dbColumn==tbColumn;
                             *  2. dbColumn != null && tbColumn == null (分库不分表)
                             *  3. dbColumn == null && tbColumn ！= null (分表不分库)
                             * </pre>
                             */

                            if (dbColumn != null && dbShardFuncPartitionStr == null) {
                                // 分表不分库
                                dbFunction = getShardFunction(schemaName, dbColumn,
                                    tableRule.getDbShardRules(),
                                    tbCountPerGroup,
                                    dbCount,
                                    useStandAlone,
                                    true);
                            }

                            if (tbColumn != null && tbShardFuncPartitionStr == null) {
                                tbFunction = getShardFunction(schemaName, tbColumn,
                                    tableRule.getTbShardRules(),
                                    tbCountPerGroup,
                                    dbCount,
                                    useStandAlone,
                                    false);
                            }
                        }

                    } catch (UnsupportedOperationException e) {
                        // e.printStackTrace();
                        fail = true;
                    }

                    if (!fail) {
                        if (dbShardFuncPartitionStr == null) {
                            if (dbColumn != null) {
                                appender.append("dbpartition by ")
                                    .append(dbFunction)
                                    .append("(`")
                                    .append(dbColumn)
                                    .append("`) ");

                                List<String> genGroups = new ArrayList<String>();
                                RuleUtils.genDBPatitionDefinition(0, 1, OptimizerContext.getContext(schemaName)
                                    .getMatrix()
                                    .getGroups(), defaultGroup, genGroups);

                                if (dbCount != genGroups.size()
                                    && (tableRule.getPartitionType() == null || !tableRule.getPartitionType()
                                    .isNoloopTime())) {
                                    appender.append("dbpartitions ").append(dbCount).append(" ");
                                }

                            }
                        } else {
                            appender.append(dbShardFuncPartitionStr).append(" ");
                        }

                        if (tbShardFuncPartitionStr == null) {
                            if (tbColumn != null && tableRule.getPartitionType() != null
                                && tableRule.getPartitionType().isNoloopTime()) {
                                appender.append("tbpartition by ")
                                    .append(tbFunction)
                                    .append("(`")
                                    .append(tbColumn)
                                    .append("`) STARTWITH ")
                                    .append(tableRule.getStart())
                                    .append(" ENDWITH ")
                                    .append(tableRule.getEnd());
                            } else if (tbFunction != null) {
                                appender.append("tbpartition by ")
                                    .append(tbFunction)
                                    .append("(`")
                                    .append(tbColumn)
                                    .append("`) tbpartitions ")
                                    .append(tbCountPerGroup);
                            }
                        } else {
                            appender.append(tbShardFuncPartitionStr);
                        }
                    }
                }

                // 热点映射
                if (tableRule != null && tableRule.getExtPartitions() != null
                    && tableRule.getExtPartitions().size() > 0) {
                    int handledCount = 0;
                    appender.append("\r\nextpartition(\r\n");
                    for (MappingRule mappingRule : tableRule.getExtPartitions()) {
                        appender.append("dbpartition ")
                            .append(mappingRule.getDb())
                            .append(" by key(")
                            .append(TStringUtil.quoteString(mappingRule.getDbKeyValue()))
                            .append(")");
                        if (StringUtils.isNotBlank(mappingRule.getTb())
                            && StringUtils.isNotBlank(mappingRule.getTbKeyValue())) {
                            appender.append(" tbpartition ")
                                .append(mappingRule.getTb())
                                .append(" by key(")
                                .append(TStringUtil.quoteString(mappingRule.getTbKeyValue()))
                                .append(")");
                        }

                        handledCount++;
                        if (handledCount < tableRule.getExtPartitions().size()) {
                            appender.append(",");
                        }
                        appender.append("\r\n");
                    }
                    appender.append(")");
                }

                String table = row.getString(0);
                String sql = row.getString(1);

                sql = LogicalShowCreateTableHandler.reorgLogicalColumnOrder(schemaName, tableName, sql);

                final GsiMetaBean gsiMeta = ExecutorContext.getContext(schemaName)
                    .getGsiManager()
                    .getGsiTableAndIndexMeta(schemaName, tableName, IndexStatus.ALL);

                boolean containImplicitColumn = false;
                boolean containAutoIncrement = false;

                // handle implicit pk
                final MySqlCreateTableStatement createTable =
                    (MySqlCreateTableStatement) SQLUtils.parseStatementsWithDefaultFeatures(sql,
                            JdbcConstants.MYSQL)
                        .get(0)
                        .clone();
                List<SQLTableElement> toAdd = Lists.newArrayList();
                List<SQLTableElement> toRemove = Lists.newArrayList();
                for (SQLTableElement sqlTableElement : createTable.getTableElementList()) {
                    if (tableMeta != null && sqlTableElement instanceof SQLColumnDefinition) {
                        SQLColumnDefinition sqlColumnDefinition = (SQLColumnDefinition) sqlTableElement;
                        if (sqlColumnDefinition.isAutoIncrement()) {
                            containAutoIncrement = true;
                        }
                        String columnName = SQLUtils.normalizeNoTrim(sqlColumnDefinition.getColumnName());
                        ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(columnName);
                        if (columnMeta != null && columnMeta.isBinaryDefault()) {
                            // handle binary default value
                            SQLHexExpr newDefaultVal = new SQLHexExpr(columnMeta.getField().getDefault());
                            sqlColumnDefinition.setDefaultExpr(newDefaultVal);
                        } else if (columnMeta != null && columnMeta.isLogicalGeneratedColumn()) {
                            // handle generated column
                            sqlColumnDefinition.setGeneratedAlawsAs(
                                new MySqlExprParser(ByteString.from(columnMeta.getField().getDefault())).expr());
                            sqlColumnDefinition.setLogical(true);
                            sqlColumnDefinition.setDefaultExpr(null);
                        }
                    }

                    if (sqlTableElement instanceof SQLColumnDefinition
                        && SqlValidatorImpl.isImplicitKey(((SQLColumnDefinition) sqlTableElement).getNameAsString())) {
                        containImplicitColumn = true;
                    }

                    if (sqlTableElement instanceof SQLColumnDefinition
                        && SqlValidatorImpl.isImplicitKey(((SQLColumnDefinition) sqlTableElement).getNameAsString())
                        && !needShowImplicitId(executionContext)) {
                        toRemove.add(sqlTableElement);
                    }

                    if (sqlTableElement instanceof SQLColumnDefinition && TableColumnUtils
                        .isHiddenColumn(executionContext, schemaName, tableName,
                            SQLUtils.normalizeNoTrim(((SQLColumnDefinition) sqlTableElement).getNameAsString()))
                        && !showCreateTable.isFull()) {
                        toRemove.add(sqlTableElement);
                    }

                    if (sqlTableElement instanceof MySqlPrimaryKey
                        && SqlValidatorImpl
                        .isImplicitKey(((MySqlPrimaryKey) sqlTableElement).getColumns().get(0).toString())
                        && !needShowImplicitId(executionContext)) {
                        toRemove.add(sqlTableElement);
                    }
                    if (sqlTableElement instanceof MysqlForeignKey) {
                        MysqlForeignKey foreignKey = (MysqlForeignKey) sqlTableElement;

                        // Only single to single table was allowed to create foreign keys.
                        String defaultGroupName = tddlRuleManager.getDefaultDbIndex().toLowerCase();
                        String phyReferencedTableName =
                            SQLUtils.normalizeNoTrim(foreignKey.getReferencedTableName().getSimpleName());
                        String fullQualifiedPhyRefTableName = defaultGroupName + "." + phyReferencedTableName;

                        Set<String> logicalTableNames =
                            tddlRuleManager.getLogicalTableNames(fullQualifiedPhyRefTableName, schemaName);
                        if (CollectionUtils.isNotEmpty(logicalTableNames)) {
                            foreignKey.getReferencedTable().setSimpleName(logicalTableNames.iterator().next());
                        }
                    }

                    // Remove index key and add foreign key if it is logical FK.
                    if (sqlTableElement instanceof MySqlKey && ((MySqlKey) sqlTableElement).getName() != null &&
                        ((MySqlKey) sqlTableElement).getName().getSimpleName() != null) {
                        final ForeignKeyData foreignKeyData =
                            tableMeta.getForeignKeys()
                                .get(SQLUtils.normalizeNoTrim(((MySqlKey) sqlTableElement).getName().getSimpleName()));
                        if (foreignKeyData != null && !foreignKeyData.isPushDown()) {
                            toRemove.add(sqlTableElement);
                            final MysqlForeignKey mysqlForeignKey = new MysqlForeignKey();
                            mysqlForeignKey.setName(
                                new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(foreignKeyData.constraint)));
                            mysqlForeignKey.setHasConstraint(true);
                            // do not show fk index name
//                            mysqlForeignKey.setIndexName(
//                                new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(foreignKeyData.indexName)));
                            mysqlForeignKey.setReferencedTable(
                                new SQLExprTableSource(foreignKeyData.refSchema.equals(schemaName) ?
                                    SqlIdentifier.surroundWithBacktick(foreignKeyData.refTableName) :
                                    SqlIdentifier.surroundWithBacktick(foreignKeyData.refSchema) + "."
                                        + SqlIdentifier.surroundWithBacktick(foreignKeyData.refTableName)));
                            mysqlForeignKey.getReferencingColumns().addAll(foreignKeyData.columns.stream()
                                .map(col -> new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(col)))
                                .collect(Collectors.toList()));
                            mysqlForeignKey.getReferencedColumns().addAll(foreignKeyData.refColumns.stream()
                                .map(col -> new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(col)))
                                .collect(Collectors.toList()));
                            if (foreignKeyData.onDelete != null && !foreignKeyData.onDelete.equals(
                                ForeignKeyData.ReferenceOptionType.NO_ACTION)) {
                                mysqlForeignKey.setOnDelete(
                                    MysqlForeignKey.Option.fromString(foreignKeyData.onDelete.getText()));
                            }
                            if (foreignKeyData.onUpdate != null && !foreignKeyData.onUpdate.equals(
                                ForeignKeyData.ReferenceOptionType.NO_ACTION)) {
                                mysqlForeignKey.setOnUpdate(
                                    MysqlForeignKey.Option.fromString(foreignKeyData.onUpdate.getText()));
                            }
                            if (showCreateTable.isFull()) {
                                mysqlForeignKey.setPushDown(
                                    MysqlForeignKey.PushDown.fromBoolean(foreignKeyData.isPushDown()));
                            }
                            toAdd.add(mysqlForeignKey);
                        }
                    }

                    // Remove duplicate foreign key if it is identical with logical one.
                    if (sqlTableElement instanceof MysqlForeignKey) {
                        // Enable constraint name by default.
                        final ForeignKeyData foreignKeyData =
                            tableMeta.getForeignKeys()
                                .get(SQLUtils.normalizeNoTrim(
                                    ((MysqlForeignKey) sqlTableElement).getName().getSimpleName()));
                        if (((MysqlForeignKey) sqlTableElement).getName() != null) {
                            ((MysqlForeignKey) sqlTableElement).setHasConstraint(true);
                        }
                        ((MysqlForeignKey) sqlTableElement).setReferencedTableName(new SQLIdentifierExpr(
                            SqlIdentifier.surroundWithBacktick(foreignKeyData.refTableName)));
                        ((MysqlForeignKey) sqlTableElement).setName(new SQLIdentifierExpr(
                            SqlIdentifier.surroundWithBacktick(foreignKeyData.constraint)));
                        if (showCreateTable.isFull()) {
                            ((MysqlForeignKey) sqlTableElement).setPushDown(
                                MysqlForeignKey.PushDown.fromBoolean(foreignKeyData.isPushDown()));
                        }
                    }
                }
                createTable.getTableElementList().removeAll(toRemove);
                createTable.getTableElementList().addAll(toAdd);

                // handle auto partition.
                if (tableMeta != null) {
                    createTable.setPrefixPartition(tableMeta.isAutoPartition());
                    // Change local index to explicit local index.
                    if (tableMeta.isAutoPartition()) {
                        for (SQLTableElement element : createTable.getTableElementList()) {
                            if (element instanceof MySqlKey && !(element instanceof MySqlPrimaryKey)) {
                                // Unique and key.
                                final MySqlKey key = (MySqlKey) element;
                                if (!key.getIndexDefinition().isLocal() && !key.getIndexDefinition().isGlobal()
                                    && !key
                                    .getIndexDefinition().isClustered()) {
                                    key.getIndexDefinition().setLocal(true);
                                }
                            } else if (element instanceof MySqlTableIndex) {
                                // Index.
                                final MySqlTableIndex index = (MySqlTableIndex) element;
                                if (!index.getIndexDefinition().isLocal() && !index.getIndexDefinition().isGlobal()
                                    && !index.getIndexDefinition().isClustered()) {
                                    index.setLocal(true);
                                }
                            }
                        }
                    }
                }
                if (gsiMeta.withGsi(tableName)) {
                    final List<SQLTableElement> gsiDefs;
                    gsiDefs = buildGsiDefs(schemaName, gsiMeta, tableName, showCreateTable.isFull());
                    createTable.getTableElementList().addAll(gsiDefs);
                }

                //handle lbac attr
                buildLBACAttr(createTable, schemaName, tableName);

                /**
                 * fix autoIncrement 显示问题:
                 * 1. 值取自sequence manager里的真实sequence值, 而非物理表上自带的autoIncrement值
                 * 2. 如果存在`_drds_implicit_id_`列, 则不显示autoIncrement值
                 * 3. 对于group, new, simple sequence, 如果当前table还未触发sequence.nextval, 则不显示autoIncrement值(与mysql保持一致)
                 * */
                SQLAssignItem autoIncrementOption = createTable
                    .getTableOptions()
                    .stream()
                    .filter(option -> {
                        if (option instanceof SQLAssignItem) {
                            if (option.getTarget().toString().equalsIgnoreCase("AUTO_INCREMENT")) {
                                return true;
                            }
                        }
                        return false;
                    })
                    .findFirst()
                    .orElse(null);

                if (!containImplicitColumn && containAutoIncrement) {
                    String sequenceName = SequenceAttribute.AUTO_SEQ_PREFIX + tableName;

                    SequencesAccessor sequencesAccessor = new SequencesAccessor();

                    boolean hide = false;
                    List<SequencesRecord> sequence = null;
                    try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                        sequencesAccessor.setConnection(metaDbConn);
                        String whereClause = String.format(" where name = \"%s\"", sequenceName);
                        sequence = sequencesAccessor.show(schemaName, whereClause);
                    } catch (SQLException e) {
                        logger.error("failed to query sequence info", e);
                        hide = true;
                    }

                    Long valueInMeta = null;
                    if (sequence != null && sequence.size() == 1) {
                        SequencesRecord sequencesRecord = sequence.get(0);
                        SequenceAttribute.Type type = Type.fromString(sequencesRecord.type);
                        if (type == Type.NEW || type == Type.SIMPLE) {
                            Long startWith = null;
                            boolean parseSucceed = true;
                            try {
                                valueInMeta = Long.parseLong(sequencesRecord.value);
                                startWith = Long.parseLong(sequencesRecord.startWith);
                            } catch (Exception ignore) {
                                hide = true;
                                parseSucceed = false;
                            }

                            if (parseSucceed && valueInMeta.equals(startWith)) {
                                hide = true;
                            }
                        } else if (type == Type.GROUP) {
                            Long unitCount = null, unitIndex = null, innerStep = null;
                            boolean parseSucceed = true;
                            try {
                                valueInMeta = Long.parseLong(sequencesRecord.value);
                                unitCount = Long.parseLong(sequencesRecord.unitCount);
                                unitIndex = Long.parseLong(sequencesRecord.unitIndex);
                                innerStep = Long.parseLong(sequencesRecord.innerStep);
                            } catch (Exception ignore) {
                                hide = true;
                                parseSucceed = false;
                            }
                            //value为初始值，代表sequence尚未被用, 可以隐藏
                            if (parseSucceed) {
                                Long initBound = (unitIndex + unitCount) * innerStep;
                                if (valueInMeta < initBound) {
                                    hide = true;
                                }
                            }
                        }
                    } else {
                        hide = true;
                    }

                    if (!hide) {
                        Long seqVal = SequenceManagerProxy.getInstance()
                            .currValue(schemaName, sequenceName) + 1L;
                        if (valueInMeta != null) {
                            seqVal = max(seqVal, valueInMeta);
                        }
                        if (autoIncrementOption != null) {
                            autoIncrementOption.setValue(new SQLIntegerExpr(seqVal));
                        } else {
                            autoIncrementOption = new SQLAssignItem();
                            autoIncrementOption.setTarget(new SQLIdentifierExpr("AUTO_INCREMENT"));
                            autoIncrementOption.setValue(new SQLIntegerExpr(seqVal));
                            createTable.getTableOptions().add(autoIncrementOption);
                        }
                    } else {
                        Iterator<SQLAssignItem> iterator = createTable.getTableOptions().iterator();
                        while (iterator.hasNext()) {
                            SQLAssignItem option = iterator.next();
                            if (option.getTarget().toString().equalsIgnoreCase("AUTO_INCREMENT")) {
                                iterator.remove();
                            }
                        }
                    }
                } else {
                    Iterator<SQLAssignItem> iterator = createTable.getTableOptions().iterator();
                    while (iterator.hasNext()) {
                        SQLAssignItem option = iterator.next();
                        if (option.getTarget().toString().equalsIgnoreCase("AUTO_INCREMENT")) {
                            iterator.remove();
                        }
                    }
                }

//                sql = createTable.toString();
                final boolean outputMySQLIndent =
                    executionContext.getParamManager().getBoolean(ConnectionParams.OUTPUT_MYSQL_INDENT);
                sql = createTable.toSqlString(false, outputMySQLIndent);

                // Sharding table or single table with broadcast
                if (tableRule != null) {
                    // Check if any implicit key exists. If yes, it means that
                    // there is no auto_increment column use specifies, so we
                    // shouldn't replace sql with extended sequence syntax.
                    boolean hasNoImplicitKey = toRemove.isEmpty();
                    // Check if any AUTO_INCREMENT column exists.
                    if (hasNoImplicitKey &&
                        StringUtils.containsIgnoreCase(sql, SequenceAttribute.NATIVE_AUTO_INC_SYNTAX)) {
                        // Get the corresponding sequence type.
                        String seqName = SequenceAttribute.AUTO_SEQ_PREFIX + tableName;
                        try {
                            String sequenceName = SequenceAttribute.AUTO_SEQ_PREFIX + tableName;
                            SequencesAccessor sequencesAccessor = new SequencesAccessor();
                            List<SequencesRecord> sequence = null;
                            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                                sequencesAccessor.setConnection(metaDbConn);
                                String whereClause = String.format(" where name = \"%s\"", sequenceName);
                                sequence = sequencesAccessor.show(schemaName, whereClause);
                            } catch (SQLException e) {
                                logger.error("failed to query sequence info", e);
                            }
                            Type seqType =
                                sequence != null && sequence.size() == 1 ? Type.fromString(sequence.get(0).type) :
                                    Type.NA;
                            if (seqType != Type.NA) {
                                // Replace it with extended syntax.
                                String replacement = SequenceAttribute.EXTENDED_AUTO_INC_SYNTAX + seqType;
                                sql = StringUtils.replaceOnce(sql,
                                    SequenceAttribute.NATIVE_AUTO_INC_SYNTAX,
                                    replacement);
                            }
                        } catch (Exception e) {
                            logger.error("Failed to check sequence '" + seqName + "'. Caused by: " + e.getMessage(), e);
                        }
                    }
                }

                LocalityManager lm = LocalityManager.getInstance();
                LocalityInfo localityInfo = lm.getLocalityOfTable(tableMeta.getId());
                if (localityInfo != null) {
                    LocalityDesc localityDesc = LocalityInfoUtils.parse(localityInfo.getLocality());
                    if (!localityDesc.holdEmptyDnList()) {
                        sql += "\n" + localityDesc.showCreate();
                    }
                }

                // Have to replace twice for compatibility with
                // 'lower_case_table_names' settings.
                String replacedDDL = StringUtils.replaceOnce(sql,
                    "`" + StringUtils.replace(table.toLowerCase(), "`", "``") + "`",
                    "`" + StringUtils.replace(tableName.toLowerCase(), "`", "``") + "`")
                    + " ";
                replacedDDL = StringUtils.replaceOnce(replacedDDL,
                    "`" + StringUtils.replace(table, "`", "``") + "`",
                    "`" + StringUtils.replace(tableName, "`", "``") + "`") + " ";

                replacedDDL = replacedDDL + appender.toString();
                result.addRow(new Object[] {tableName, replacedDDL});
            }

            return result;
        } finally {
            // 关闭cursor
            if (cursor != null) {
                cursor.close(new ArrayList<Throwable>());
            }
        }
    }

    public static void buildLBACAttr(MySqlCreateTableStatement createTable, String schemaName, String tableName) {
        //for unit test
        if (MetaDbDataSource.getInstance() == null) {
            return;
        }
        //handle lbac attr
        LBACSecurityPolicy policy = LBACSecurityManager.getInstance().getTablePolicy(schemaName, tableName);
        if (policy != null) {
            String policyName = policy.getPolicyName();
            createTable.addOption("security policy", new SQLIdentifierExpr(policyName));
            for (SQLTableElement sqlTableElement : createTable.getTableElementList()) {
                if (sqlTableElement instanceof SQLColumnDefinition) {
                    String columnName = ((SQLColumnDefinition) sqlTableElement).getColumnName();
                    if (columnName.startsWith("`")) {
                        columnName = columnName.substring(1, columnName.length() - 1);
                    }
                    LBACSecurityLabel
                        label = LBACSecurityManager.getInstance().getColumnLabel(schemaName, tableName, columnName);
                    if (label != null) {
                        ((SQLColumnDefinition) sqlTableElement).setSecuredWith(
                            new SQLIdentifierExpr(label.getLabelName()));
                    }
                }
            }
        }
    }

    public List<SQLTableElement> buildGsiDefs(String schemaName, GsiMetaBean gsiMeta, String mainTableName,
                                              boolean full) {
        final GsiMetaManager.GsiTableMetaBean mainTableMeta = gsiMeta.getTableMeta().get(mainTableName);
        List<SQLTableElement> gsiDefs = new ArrayList<>(mainTableMeta.indexMap.size());
        for (Map.Entry<String, GsiMetaManager.GsiIndexMetaBean> entry : mainTableMeta.indexMap.entrySet()) {
            final String indexName = entry.getKey();
            final GsiMetaManager.GsiIndexMetaBean indexMeta = entry.getValue();
            final GsiMetaManager.GsiTableMetaBean indexTableMeta = gsiMeta.getTableMeta().get(indexName);

            // Ignore GSI which is not public.
            if (indexMeta.indexStatus != IndexStatus.PUBLIC) {
                continue;
            }

            final List<SQLSelectOrderByItem> indexColumns = new ArrayList<>(indexMeta.indexColumns.size());
            final List<SQLName> coveringColumns = new ArrayList<>(indexMeta.coveringColumns.size());
            final SQLCharExpr comment =
                TStringUtil.isEmpty(indexMeta.indexComment) ? null : new SQLCharExpr(indexMeta.indexComment);
            final SQLExpr tbPartitions =
                indexTableMeta.tbPartitionCount == null ? null : new SQLNumberExpr(indexTableMeta.tbPartitionCount);

            final SQLExpr dbPartitionBy = buildPartitionBy(indexTableMeta, false);

            final SQLExpr tbPartitionBy = buildPartitionBy(indexTableMeta, true);
            final boolean clusteredIndex = indexMeta.clusteredIndex;

            for (GsiMetaManager.GsiIndexColumnMetaBean indexColumn : indexMeta.indexColumns) {
                SQLSelectOrderByItem orderByItem = new SQLSelectOrderByItem();
                if (null != indexColumn.subPart && indexColumn.subPart > 0) {
                    final SQLMethodInvokeExpr methodInvoke =
                        new SQLMethodInvokeExpr("`" + indexColumn.columnName + "`");
                    methodInvoke.addArgument(new SQLIntegerExpr(indexColumn.subPart));
                    orderByItem.setExpr(methodInvoke);
                } else {
                    orderByItem.setExpr(new SQLIdentifierExpr("`" + indexColumn.columnName + "`"));
                }
                if (TStringUtil.equals("A", indexColumn.collation)) {
                    orderByItem.setType(SQLOrderingSpecification.ASC);
                } else if (TStringUtil.equals("D", indexColumn.collation)) {
                    orderByItem.setType(SQLOrderingSpecification.DESC);
                }
                // TODO collation

                indexColumns.add(orderByItem);
            }

            for (GsiMetaManager.GsiIndexColumnMetaBean coveringColumn : indexMeta.coveringColumns) {
                if (SqlValidatorImpl.isImplicitKey(coveringColumn.columnName)) {
                    continue;
                }

                SQLName covering = new SQLIdentifierExpr("`" + coveringColumn.columnName + "`");
                coveringColumns.add(covering);
            }

            // Get show name.
            final String showName = DbInfoManager.getInstance().isNewPartitionDb(schemaName) ?
                TddlSqlToRelConverter.unwrapGsiName(indexMeta.indexName) : indexMeta.indexName;
            final String preifx = showName.equals(indexMeta.indexName) ? "" : "/* " + indexMeta.indexName + " */ ";

            if (indexMeta.nonUnique) {
// index
                final MySqlTableIndex tableIndex = new MySqlTableIndex();
                tableIndex.getIndexDefinition().setIndex(true);

                tableIndex.setDbPartitionBy(dbPartitionBy);
                tableIndex.setTablePartitionBy(tbPartitionBy);
                tableIndex.setTablePartitions(tbPartitions);
                // get/setIndexType of MySqlTableIndex is ambiguous since
                // refactor of DDL, because of legacy code.
                // Use getIndexDefinition().getOptions().setIndexType.
                tableIndex.getIndexDefinition().getOptions().setIndexType(indexMeta.indexType);
                tableIndex.setName(preifx + SqlIdentifier.surroundWithBacktick(showName));
                tableIndex.setComment(comment);
                tableIndex.getColumns().addAll(indexColumns);
                if (clusteredIndex) {
                    tableIndex.setClustered(true);
                } else {
                    tableIndex.setGlobal(true);
                    tableIndex.getCovering().addAll(coveringColumns);
                }

                if (indexMeta.visibility == IndexVisibility.VISIBLE) {
                    tableIndex.getIndexDefinition().setVisible(true);
                } else {
                    tableIndex.getIndexDefinition().setVisible(false);
                }
                gsiDefs.add(tableIndex);

            } else {
// unique index
                final MySqlUnique tableIndex = new MySqlUnique();
                tableIndex.getIndexDefinition().setType("UNIQUE");
                tableIndex.getIndexDefinition().setKey(true);
                tableIndex.setDbPartitionBy(dbPartitionBy);
                tableIndex.setTablePartitionBy(tbPartitionBy);
                tableIndex.setTablePartitions(tbPartitions);
                tableIndex.setIndexType(indexMeta.indexType);
                tableIndex.setName(preifx + SqlIdentifier.surroundWithBacktick(showName));
                tableIndex.setComment(comment);
                tableIndex.getColumns().addAll(indexColumns);
                if (clusteredIndex) {
                    tableIndex.setClustered(true);
                } else {
                    tableIndex.setGlobal(true);
                    tableIndex.getCovering().addAll(coveringColumns);
                }

                if (indexMeta.visibility == IndexVisibility.VISIBLE) {
                    tableIndex.getIndexDefinition().setVisible(true);
                } else {
                    tableIndex.getIndexDefinition().setVisible(false);
                }

                gsiDefs.add(tableIndex);
            } // end of else

        } // end of for
        return gsiDefs;
    }

    public SQLExpr buildPartitionBy(GsiMetaManager.GsiTableMetaBean indexTableMeta, boolean onTable) {
        final String policy = onTable ? indexTableMeta.tbPartitionPolicy : indexTableMeta.dbPartitionPolicy;
        final String key = onTable ? indexTableMeta.tbPartitionKey : indexTableMeta.dbPartitionKey;

        return buildPartitionBy(policy, key, onTable);
    }

    public static SQLExpr buildPartitionBy(String policy, String key, boolean onTable) {
        if (TStringUtil.isBlank(policy)) {
            return null;
        }

        final boolean singleParam = isSingleParam(policy);

        if (!singleParam) {
            try {
                final MySqlCreateTableParser createParser =
                    new MySqlCreateTableParser(new MySqlExprParser((onTable ? "TBPARTITION" : "DBPARTITION")
                        + " BY "
                        + policy));
                final SQLPartitionBy partitionBy = createParser.parsePartitionBy();
                if (partitionBy instanceof SQLPartitionByRange) {
                    final SQLPartitionByRange sqlPartitionBy = (SQLPartitionByRange) partitionBy;
                    return sqlPartitionBy.getInterval();
                } else if (partitionBy instanceof SQLPartitionByHash) {
                    SQLMethodInvokeExpr partitionByInvoke = null;
                    if (((SQLPartitionByHash) partitionBy).isUnique()) {
                        final SQLMethodInvokeExpr finalPartitionBy = new SQLMethodInvokeExpr("UNI_HASH");
                        partitionBy.getColumns().forEach(finalPartitionBy::addArgument);

                        partitionByInvoke = finalPartitionBy;
                    } else {
                        partitionByInvoke = new SQLMethodInvokeExpr("HASH");
                        partitionByInvoke.addArgument(new SQLIdentifierExpr("`" + key + "`"));
                    }
                    return partitionByInvoke;
                }
            } catch (Exception ignored) {
                // cannot parse tbpartition policy
            }
        }

        final SQLMethodInvokeExpr partitionBy = new SQLMethodInvokeExpr(policy);
        partitionBy.addArgument(new SQLIdentifierExpr("`" + key + "`"));
        return partitionBy;
    }

    public static boolean isSingleParam(String partitionPolicy) {
        return TStringUtil.startsWithIgnoreCase(partitionPolicy, "hash")
            || TStringUtil.startsWithIgnoreCase(partitionPolicy, "yyyymm_opt")
            || TStringUtil.startsWithIgnoreCase(partitionPolicy, "yyyydd_opt")
            || TStringUtil.startsWithIgnoreCase(partitionPolicy, "yyyyweek_opt")
            || TStringUtil.startsWithIgnoreCase(partitionPolicy, "yyyymm")
            || TStringUtil.startsWithIgnoreCase(partitionPolicy, "yyyydd")
            || TStringUtil.startsWithIgnoreCase(partitionPolicy, "yyyyweek")
            || TStringUtil.startsWithIgnoreCase(partitionPolicy, "mm")
            || TStringUtil.startsWithIgnoreCase(partitionPolicy, "dd")
            || TStringUtil.startsWithIgnoreCase(partitionPolicy, "mmdd")
            || TStringUtil.startsWithIgnoreCase(partitionPolicy, "week");
    }

    private boolean needShowImplicitId(ExecutionContext executionContext) {
        Object value = executionContext.getExtraCmds().get(ConnectionProperties.SHOW_IMPLICIT_ID);
        return value != null && Boolean.parseBoolean(value.toString());
    }

    private Cursor showViews(RelNode logicalPlan, String schemaName, String tableName,
                             ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowCreateTable showCreateTable = (SqlShowCreateTable) show.getNativeSqlNode();
        try {
            ViewManager viewManager;
            if (InformationSchema.NAME.equalsIgnoreCase(schemaName)) {
                viewManager = InformationSchemaViewManager.getInstance();
            } else if (RelUtils.informationSchema(showCreateTable.getTableName())) {
                viewManager = InformationSchemaViewManager.getInstance();
            } else if (RelUtils.mysqlSchema(showCreateTable.getTableName())) {
                viewManager = MysqlSchemaViewManager.getInstance();
            } else if (RelUtils.performanceSchema(showCreateTable.getTableName())) {
                viewManager = PerformanceSchemaViewManager.getInstance();
            } else {
                viewManager = OptimizerContext.getContext(schemaName).getViewManager();
            }

            SystemTableView.Row row = viewManager.select(tableName);
            if (row != null) {
                ArrayResultCursor resultCursor = new ArrayResultCursor(tableName);
                // | View | Create View | character_set_client | collation_connection |
                resultCursor.addColumn("View", DataTypes.StringType, false);
                resultCursor.addColumn("Create View", DataTypes.StringType, false);
                resultCursor.addColumn("character_set_client", DataTypes.StringType, false);
                resultCursor.addColumn("collation_connection", DataTypes.StringType, false);
                boolean printPlan = row.getPlan() != null && row.getPlanType() != null;
                if (printPlan) {
                    // | PLAN | PLAN_TYPE |
                    resultCursor.addColumn("PLAN", DataTypes.StringType, false);
                    resultCursor.addColumn("PLAN_TYPE", DataTypes.StringType, false);
                }

                String createView = row.isVirtual() ? "[VIRTUAL_VIEW] " + row.getViewDefinition() :
                    "CREATE VIEW `" + tableName + "` AS " + row.getViewDefinition();

                if (printPlan) {
                    String explainString;
                    RelOptSchema relOptSchema =
                        SqlConverter.getInstance(schemaName, executionContext).getCatalog();
                    try {
                        explainString = RelUtils.toString(PlanManagerUtil
                            .jsonToRelNode(row.getPlan(), logicalPlan.getCluster(), relOptSchema));
                    } catch (Throwable throwable) {
                        throwable.printStackTrace();
                        explainString = throwable.getMessage();
                    }

                    resultCursor.addRow(new Object[] {
                        tableName,
                        createView,
                        "utf8",
                        "utf8_general_ci", "\n" + explainString, row.getPlanType()});
                } else {
                    resultCursor.addRow(new Object[] {
                        tableName,
                        createView,
                        "utf8",
                        "utf8_general_ci"});
                }
                return resultCursor;
            }
        } catch (Throwable t2) {
            // pass
        }
        return null;
    }
}
