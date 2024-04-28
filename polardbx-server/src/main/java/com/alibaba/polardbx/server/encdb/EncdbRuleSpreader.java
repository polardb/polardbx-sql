package com.alibaba.polardbx.server.encdb;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRule;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRuleManager;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.ddl.CreateTable;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlChangeColumn;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlCreateView;
import org.apache.calcite.sql.SqlDropColumn;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlDropView;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlModifyColumn;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlRenameTable;
import org.apache.calcite.sql.SqlRenameTables;
import org.apache.calcite.sql.SqlReplace;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * encdb加密规则扩散
 *
 * @author pangzhaoxing
 */
public class EncdbRuleSpreader {

    private static final Logger logger = LoggerFactory.getLogger(EncdbRuleSpreader.class);

    public static void maySpreadEncRules(ExecutionContext ec) {
        if (!InstConfUtil.getBool(ConnectionParams.ENABLE_ENCDB)) {
            return;
        }

        if (ec.getFinalPlan().isExplain()) {
            return;
        }

        try {
            SqlNode sqlNode = ec.getFinalPlan().getAst();
            if (sqlNode instanceof SqlCreateTable) {
                maySpreadEncRules((SqlCreateTable) sqlNode, ec);
            } else if (sqlNode instanceof SqlCreateView) {
                maySpreadEncRules((SqlCreateView) sqlNode, ec);
            } else if (sqlNode instanceof SqlReplace) {
                maySpreadEncRules((SqlReplace) sqlNode, ec);
            } else if (sqlNode instanceof SqlInsert) {
                maySpreadEncRules((SqlInsert) sqlNode, ec);
            } else if (sqlNode instanceof SqlRenameTable) {
                maySpreadEncRules((SqlRenameTable) sqlNode, ec);
            } else if (sqlNode instanceof SqlRenameTables) {
                maySpreadEncRules((SqlRenameTables) sqlNode, ec);
            } else if (sqlNode instanceof SqlDropTable) {
                maySpreadEncRules((SqlDropTable) sqlNode, ec);
            } else if (sqlNode instanceof SqlDropView) {
                maySpreadEncRules((SqlDropView) sqlNode, ec);
            } else if (sqlNode instanceof SqlAlterTable) {
                for (SqlAlterSpecification sqlAlterSpecification : ((SqlAlterTable) sqlNode).getAlters()) {
                    if (sqlAlterSpecification instanceof SqlChangeColumn) {
                        maySpreadEncRules((SqlChangeColumn) sqlAlterSpecification, ec);
                    } else if (sqlAlterSpecification instanceof SqlDropColumn) {
                        maySpreadEncRules((SqlDropColumn) sqlAlterSpecification, ec);
                    }
                }
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static void maySpreadEncRules(SqlDropTable sqlDropTable, ExecutionContext ec) {
        if (!(sqlDropTable.getTargetTable() instanceof SqlIdentifier)) {
            sqlDropTable = (SqlDropTable) new FastsqlParser().parse(ec.getOriginSql()).get(0);
        }

        String schema = ec.getSchemaName();
        String table;
        SqlIdentifier targetTable = (SqlIdentifier) sqlDropTable.getTargetTable();
        if (targetTable.names.size() > 1) {
            schema = targetTable.names.get(0);
            table = targetTable.names.get(1);
        } else {
            table = targetTable.names.get(0);
        }
        Set<String> spreadEncRules = EncdbRuleManager.getInstance()
            .getRuleMatchTree().getSpecificTableRules(schema, table)
            .stream().filter(ruleName -> ruleName.startsWith(EncdbRuleManager.ENCDB_SPREADED_RULE_))
            .collect(Collectors.toSet());
        EncdbRuleManager.getInstance().deleteEncRules(spreadEncRules);
    }

    private static void maySpreadEncRules(SqlDropView sqlDropView, ExecutionContext ec) {
        if (!(sqlDropView.getTargetTable() instanceof SqlIdentifier)) {
            sqlDropView = (SqlDropView) new FastsqlParser().parse(ec.getOriginSql()).get(0);
        }

        String schema = ec.getSchemaName();
        String table;
        SqlIdentifier targetTable = (SqlIdentifier) sqlDropView.getTargetTable();
        if (targetTable.names.size() > 1) {
            schema = targetTable.names.get(0);
            table = targetTable.names.get(1);
        } else {
            table = targetTable.names.get(0);
        }
        Set<String> spreadEncRules = EncdbRuleManager.getInstance()
            .getRuleMatchTree().getSpecificTableRules(schema, table)
            .stream().filter(ruleName -> ruleName.startsWith(EncdbRuleManager.ENCDB_SPREADED_RULE_))
            .collect(Collectors.toSet());
        EncdbRuleManager.getInstance().deleteEncRules(spreadEncRules);
    }

    private static void maySpreadEncRules(SqlCreateTable sqlCreateTable, ExecutionContext ec) {
        if (sqlCreateTable.getQuery() == null || !(sqlCreateTable.getQuery() instanceof SqlSelect)) {
            return;
        }
        if (!(sqlCreateTable.getTargetTable() instanceof SqlIdentifier)) {
            sqlCreateTable = (SqlCreateTable) new FastsqlParser().parse(ec.getOriginSql()).get(0);
        }

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(ec);
        ExecutionPlan selectPlan = Planner.getInstance().getPlan(sqlCreateTable.getQuery(), plannerContext);
        List<Set<String>> columnMatchRulesList = EncdbRuleManager.getInstance().getRuleMatchTree()
            .getColumnMatchRulesList(CalciteUtils.buildOriginColumnNames(selectPlan.getPlan()));
        if (columnMatchRulesList != null) {
            String schema = ec.getSchemaName();
            String table;
            SqlIdentifier targetTable = (SqlIdentifier) sqlCreateTable.getTargetTable();
            if (targetTable.names.size() > 1) {
                schema = targetTable.names.get(0);
                table = targetTable.names.get(1);
            } else {
                table = targetTable.names.get(0);
            }
            List<String> columns = sqlCreateTable.getColDefs()
                .stream()
                .map(pair -> pair.getKey().getSimple())
                .collect(Collectors.toList());
            spreadEncRules(schema, table, columns, columnMatchRulesList);
        }
    }

    private static void maySpreadEncRules(SqlCreateView sqlCreateView, ExecutionContext ec) {
        if (sqlCreateView.getQuery() == null || !(sqlCreateView.getQuery() instanceof SqlSelect)) {
            return;
        }
        if (!(sqlCreateView.getTargetTable() instanceof SqlIdentifier)) {
            sqlCreateView = (SqlCreateView) new FastsqlParser().parse(ec.getOriginSql()).get(0);
        }

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(ec);
        ExecutionPlan selectPlan = Planner.getInstance().getPlan(sqlCreateView.getQuery(), plannerContext);
        List<Set<String>> columnMatchRulesList = EncdbRuleManager.getInstance().getRuleMatchTree()
            .getColumnMatchRulesList(CalciteUtils.buildOriginColumnNames(selectPlan.getPlan()));
        if (columnMatchRulesList != null) {
            String schema = ec.getSchemaName();
            String table;
            SqlIdentifier targetTable = (SqlIdentifier) sqlCreateView.getTargetTable();
            if (targetTable.names.size() > 1) {
                schema = targetTable.names.get(0);
                table = targetTable.names.get(1);
            } else {
                table = targetTable.names.get(0);
            }
            List<String> columns = selectPlan.getCursorMeta().getColumns()
                .stream()
                .map(ColumnMeta::getName)
                .collect(Collectors.toList());
            spreadEncRules(schema, table, columns, columnMatchRulesList);
        }
    }

    private static void maySpreadEncRules(SqlInsert sqlInsert, ExecutionContext ec) {
        if (sqlInsert.getSource() == null || !(sqlInsert.getSource() instanceof SqlSelect)) {
            return;
        }
        if (!(sqlInsert.getTargetTable() instanceof SqlIdentifier)) {
            sqlInsert = (SqlInsert) new FastsqlParser().parse(ec.getOriginSql()).get(0);
        }

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(ec);
        ExecutionPlan selectPlan = Planner.getInstance().getPlan(sqlInsert.getSource(), plannerContext);
        List<Set<String>> columnMatchRulesList = EncdbRuleManager.getInstance().getRuleMatchTree()
            .getColumnMatchRulesList(CalciteUtils.buildOriginColumnNames(selectPlan.getPlan()));
        if (columnMatchRulesList != null) {
            String schema = ec.getSchemaName();
            String table;
            SqlIdentifier targetTable = (SqlIdentifier) sqlInsert.getTargetTable();
            if (targetTable.names.size() > 1) {
                schema = targetTable.names.get(0);
                table = targetTable.names.get(1);
            } else {
                table = targetTable.names.get(0);
            }
            List<String> columns;
            if (sqlInsert.getTargetColumnList() == null) {
                columns = getAllColumnNames(schema, table);
            } else {
                columns = sqlInsert.getTargetColumnList().getList()
                    .stream().map(node -> ((SqlIdentifier) node).getSimple()).collect(Collectors.toList());
            }
            spreadEncRules(schema, table, columns, columnMatchRulesList);
        }
    }

    private static void maySpreadEncRules(SqlReplace sqlReplace, ExecutionContext ec) {
        if (sqlReplace.getSource() == null || !(sqlReplace.getSource() instanceof SqlSelect)) {
            return;
        }
        if (!(sqlReplace.getTargetTable() instanceof SqlIdentifier)) {
            sqlReplace = (SqlReplace) new FastsqlParser().parse(ec.getOriginSql()).get(0);
        }

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(ec);
        ExecutionPlan selectPlan = Planner.getInstance().getPlan(sqlReplace.getSource(), plannerContext);
        List<Set<String>> columnMatchRulesList = EncdbRuleManager.getInstance().getRuleMatchTree()
            .getColumnMatchRulesList(CalciteUtils.buildOriginColumnNames(selectPlan.getPlan()));
        if (columnMatchRulesList != null) {
            String schema = ec.getSchemaName();
            String table;
            SqlIdentifier targetTable = (SqlIdentifier) sqlReplace.getTargetTable();
            if (targetTable.names.size() > 1) {
                schema = targetTable.names.get(0);
                table = targetTable.names.get(1);
            } else {
                table = targetTable.names.get(0);
            }
            List<String> columns;
            if (sqlReplace.getTargetColumnList() == null) {
                columns = getAllColumnNames(schema, table);
            } else {
                columns = sqlReplace.getTargetColumnList().getList()
                    .stream().map(node -> ((SqlIdentifier) node).getSimple()).collect(Collectors.toList());
            }
            spreadEncRules(schema, table, columns, columnMatchRulesList);
        }
    }

    private static void maySpreadEncRules(SqlChangeColumn sqlChangeColumn, ExecutionContext ec) {
        String schema = ec.getSchemaName();
        String table = null;
        if (sqlChangeColumn.getOriginTableName().names.size() > 1) {
            schema = sqlChangeColumn.getOriginTableName().names.get(0);
            table = sqlChangeColumn.getOriginTableName().names.get(1);
        } else {
            table = sqlChangeColumn.getOriginTableName().names.get(0);
        }
        String originColumn = sqlChangeColumn.getOldName().getSimple();
        String newColumn = sqlChangeColumn.getNewName().getSimple();

        String matchRule = EncdbRuleManager.getInstance().getRuleMatchTree().match(schema, table, originColumn);
        if (matchRule == null) {
            return;
        }

        spreadEncRules(schema, table, Collections.singletonList(newColumn),
            Collections.singletonList(Collections.singleton(matchRule)));
    }

    private static void maySpreadEncRules(SqlDropColumn sqlDropColumn, ExecutionContext ec) {
        String schema = ec.getSchemaName();
        String table = null;
        if (sqlDropColumn.getOriginTableName().names.size() > 1) {
            schema = sqlDropColumn.getOriginTableName().names.get(0);
            table = sqlDropColumn.getOriginTableName().names.get(1);
        } else {
            table = sqlDropColumn.getOriginTableName().names.get(0);
        }
        String column = sqlDropColumn.getColName().getSimple();

        String ruleName = EncdbRuleManager.getInstance().getRuleMatchTree().match(schema, table, column);
        if (ruleName == null || !ruleName.startsWith(EncdbRuleManager.ENCDB_SPREADED_RULE_)) {
            return;
        }
        EncdbRule encdbRule = EncdbRuleManager.getInstance().getEncRule(ruleName);
        if (encdbRule.getCols().size() == 1) {
            EncdbRuleManager.getInstance().deleteEncRule(ruleName);
        } else {
            EncdbRule updateRule = encdbRule.clone();
            updateRule.getCols().remove(column);
            EncdbRuleManager.getInstance().replaceEncRules(Collections.singletonList(updateRule));
        }

    }

    private static void maySpreadEncRules(SqlRenameTables sqlRenameTables, ExecutionContext ec) {
        for (Pair<SqlIdentifier, SqlIdentifier> pair : sqlRenameTables.getTableNameList()) {
            maySpreadEncRulesByRenameTable(pair.left, pair.right, ec);
        }
    }

    private static void maySpreadEncRules(SqlRenameTable sqlRenameTable, ExecutionContext ec) {
        maySpreadEncRulesByRenameTable(sqlRenameTable.getOriginTableName(), sqlRenameTable.getRenamedNode(), ec);
    }

    private static void maySpreadEncRulesByRenameTable(SqlIdentifier originTableName, SqlIdentifier newTableName,
                                                       ExecutionContext ec) {
        String originSchema = ec.getSchemaName();
        String originTable = null;
        if (originTableName.names.size() > 1) {
            originSchema = originTableName.names.get(0);
            originTable = originTableName.names.get(1);
        } else {
            originTable = originTableName.names.get(0);
        }

        String newSchema = ec.getSchemaName();
        String newTable = null;
        if (newTableName.names.size() > 1) {
            newSchema = newTableName.names.get(0);
            newTable = newTableName.names.get(1);
        } else {
            newTable = newTableName.names.get(0);
        }

        if (originSchema.equalsIgnoreCase(newSchema) &&
            originTable.equalsIgnoreCase(newTable)) {
            return;
        }

        if (EncdbRuleManager.getInstance().getRuleMatchTree().fastMatch(originSchema, originTable)) {
            List<String> columns = getAllColumnNames(newSchema, newTable);//table name has changed
            List<Set<String>> matchRules = new ArrayList<>(columns.size());
            for (String col : columns) {
                String ruleName =
                    EncdbRuleManager.getInstance().getRuleMatchTree().match(originSchema, originTable, col);
                matchRules.add(ruleName == null ? Collections.emptySet() : Collections.singleton(ruleName));
            }
            spreadEncRules(newSchema, newTable, columns, matchRules);
        }

    }

    /**
     * 将encdb rules 扩散到对应的数据列之上
     * spread rule只会包含一个db和一个tb
     */
    public static void spreadEncRules(String schema, String table, List<String> columns, List<Set<String>> matchRules) {
        List<EncdbRule> spreadRules = new ArrayList<>(matchRules.size());
        Map<Set<String>, EncdbRule> cachedRules = new HashMap<>();
        Set<String> dbs = Collections.singleton(schema);
        Set<String> tbs = Collections.singleton(table);
        boolean enabled = true;
        for (int i = 0; i < matchRules.size(); i++) {
            Set<String> matchRuleSet = matchRules.get(i);
            if (matchRuleSet.isEmpty()) {
                continue;
            }
            //该列上已经拥有rule，则不再spread
            if (EncdbRuleManager.getInstance().getRuleMatchTree().match(schema, table, columns.get(i)) != null) {
                continue;
            }

            if (cachedRules.containsKey(matchRuleSet)) {
                cachedRules.get(matchRuleSet).getCols().add(columns.get(i));
                continue;
            }

            Set<String> cols = new HashSet<>();
            cols.add(columns.get(i));

            String ruleName = EncdbRuleManager.ENCDB_SPREADED_RULE_ + "from_";
            String description = "encdb rule spreaded from ";
            Set<PolarAccount> fullAccessUsers = new HashSet<>();
            Set<PolarAccount> restrictedUsers = new HashSet<>();
            for (String matchRuleName : matchRuleSet) {
                ruleName += matchRuleName + "_";
                description += matchRuleName + " ";
                EncdbRule matchRule = EncdbRuleManager.getInstance().getEncRule(matchRuleName);
                fullAccessUsers.addAll(matchRule.getFullAccessUsers());
                restrictedUsers.addAll(matchRule.getRestrictedAccessUsers());
            }

            Iterator<PolarAccount> iterator = fullAccessUsers.iterator();
            while (iterator.hasNext()) {
                PolarAccount fullAccessUser = iterator.next();
                if (restrictedUsers.contains(fullAccessUser)) {
                    iterator.remove();
                }
            }

            ruleName += System.currentTimeMillis() + "_" + ThreadLocalRandom.current().nextInt();
            EncdbRule encdbRule = new EncdbRule(ruleName, enabled, fullAccessUsers, restrictedUsers,
                dbs, tbs, cols, description);
            spreadRules.add(encdbRule);
            cachedRules.put(matchRuleSet, encdbRule);
        }

        EncdbRuleManager.getInstance().insertEncRules(spreadRules);

    }

    public static List<String> getAllColumnNames(String schema, String table) {
        GmsTableMetaManager schemaManager =
            (GmsTableMetaManager) OptimizerContext.getContext(schema).getLatestSchemaManager();
        TableMeta currentMeta = schemaManager.getTableWithNull(table);
        return currentMeta.getAllColumns().stream().map(ColumnMeta::getOriginColumnName).collect(Collectors.toList());
    }

}
