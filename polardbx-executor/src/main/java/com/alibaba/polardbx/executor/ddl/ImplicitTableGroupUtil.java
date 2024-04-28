package com.alibaba.polardbx.executor.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableGroupItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableTruncatePartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsAlterTableBroadcast;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsAlterTableSingle;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.Pair;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.partition.TablePartitionConfigUtil;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcSqlUtils.SQL_PARSE_FEATURES;
import static com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter.unwrapGsiName;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-10-20 10:27
 **/
public class ImplicitTableGroupUtil {
    @Getter
    @Setter
    private static TableGroupConfigProvider tableGroupConfigProvider = new DefaultTableGroupConfigProvider();
    public static final ThreadLocal<Map<String, String>> exchangeNamesMapping = new ThreadLocal<>();

    public static String tryAttachImplicitTableGroup(String schemaName, String tableName, String sql) {
        try {
            return tryAttachImplicitTableGroupInternal(schemaName, tableName, sql);
        } finally {
            exchangeNamesMapping.set(null);
        }
    }

    public static String tryAttachImplicitTableGroupInternal(String schemaName, String tableName, String sql) {
        if (!tableGroupConfigProvider.isNewPartitionDb(schemaName)) {
            return sql;
        }

        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> parseResult = parser.parseStatementList();
        if (!parseResult.isEmpty()) {
            boolean changed = false;
            SQLStatement statement = parseResult.get(0);

            if (statement instanceof MySqlCreateTableStatement) {
                changed = process4CreateTable(schemaName, tableName, (MySqlCreateTableStatement) statement,
                    false, null);
            } else if (statement instanceof SQLAlterTableStatement) {
                changed = process4AlterTable(schemaName, tableName, (SQLAlterTableStatement) statement,
                    false, null);
            } else if (statement instanceof SQLCreateIndexStatement) {
                changed = process4CreateIndex(schemaName, tableName, (SQLCreateIndexStatement) statement,
                    false, null);
            }

            return changed ? statement.toString(VisitorFeature.OutputHashPartitionsByRange) : sql;
        }

        return sql;
    }

    public static boolean checkSql(String schemaName, String tableName, String sql, Set<String> tgGroups) {
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> parseResult = parser.parseStatementList();
        SQLStatement statement = parseResult.get(0);

        if (statement instanceof MySqlCreateTableStatement) {
            process4CreateTable(schemaName, tableName, (MySqlCreateTableStatement) statement, true, tgGroups);
        } else if (statement instanceof SQLAlterTableStatement) {
            process4AlterTable(schemaName, tableName, (SQLAlterTableStatement) statement, true, tgGroups);
        } else if (statement instanceof SQLCreateIndexStatement) {
            process4CreateIndex(schemaName, tableName, (SQLCreateIndexStatement) statement, true, tgGroups);
        }

        return true;
    }

    public static void removeImplicitTgSyntax(SQLStatement statement) {
        if (statement instanceof MySqlCreateTableStatement) {
            removeImplicitTgForCreateTable((MySqlCreateTableStatement) statement);
        } else if (statement instanceof SQLAlterTableStatement) {
            removeImplicitTgForAlterTable((SQLAlterTableStatement) statement);
        } else if (statement instanceof SQLCreateIndexStatement) {
            SQLCreateIndexStatement createIndexStatement = (SQLCreateIndexStatement) statement;
            createIndexStatement.setTableGroup(null);
            createIndexStatement.setWithImplicitTablegroup(false);
        }
    }

    public static String rewriteTableName(String sql, String newTableName) {
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> parseResult = parser.parseStatementList();
        SQLStatement statement = parseResult.get(0);

        if (statement instanceof MySqlCreateTableStatement) {
            ((MySqlCreateTableStatement) statement).setTableName(newTableName);
        } else if (statement instanceof SQLAlterTableStatement) {
            ((SQLAlterTableStatement) statement).setName(new SQLIdentifierExpr(newTableName));
        } else if (statement instanceof SQLCreateIndexStatement) {
            ((SQLCreateIndexStatement) statement).setTable(new SQLIdentifierExpr(newTableName));
        }
        return statement.toString(VisitorFeature.OutputHashPartitionsByRange);
    }

    public static void checkAutoCreateTableGroup(TableGroupConfig tableGroupConfig, boolean isOSS,
                                                 boolean withTableGroupImplicit, boolean autoCreateTg) {
        if (tableGroupConfig == null) {
            return;
        }
        TableGroupRecord record = tableGroupConfig.getTableGroupRecord();
        if (!isOSS && record != null && !autoCreateTg && !withTableGroupImplicit) {
            throw new TddlRuntimeException(ErrorCode.ERR_AUTO_CREATE_TABLEGROUP,
                "create tablegroup automatically is not allow");
        }

    }

    public static void checkAutoCreateTableGroup(ExecutionContext ec) {
        boolean autoCreateTg = ec.getParamManager().getBoolean(ConnectionParams.ALLOW_AUTO_CREATE_TABLEGROUP);
        if (!autoCreateTg) {
            throw new TddlRuntimeException(ErrorCode.ERR_AUTO_CREATE_TABLEGROUP,
                "create tablegroup automatically is not allow");
        }

    }

    private static boolean process4CreateTable(String schemaName, String tableName,
                                               MySqlCreateTableStatement sqlCreateTableStatement, boolean forCheck,
                                               Set<String> tgGroups) {
        boolean changed = false;
        TableGroupConfig tableGroupConfig =
            tableGroupConfigProvider.getTableGroupConfig(schemaName, buildQueryTableName(tableName), false);
        if (tableGroupConfig != null && !tableGroupConfig.isManuallyCreated()) {
            if (forCheck) {
                Assert.assertNotNull(sqlCreateTableStatement.getTableGroup());
                forceCheckTgName(sqlCreateTableStatement.getTableGroup().getSimpleName(),
                    tableGroupConfig.getTableGroupRecord().getTg_name());
                Assert.assertTrue(sqlCreateTableStatement.isWithImplicitTablegroup());
                tgGroups.add(sqlCreateTableStatement.getTableGroup().getSimpleName());

                // see aone 56399827
                if (tableGroupConfig.getTableGroupRecord().isSingleTableGroup()) {
                    Assert.assertTrue(sqlCreateTableStatement.isSingle());
                }
            } else {
                tryCheckTgName(sqlCreateTableStatement.getTableGroup(), tableGroupConfig);
                sqlCreateTableStatement.setTableGroup(
                    new SQLIdentifierExpr(tableGroupConfig.getTableGroupRecord().getTg_name()));
                sqlCreateTableStatement.setWithImplicitTablegroup(true);

                // see aone 56399827
                if (tableGroupConfig.getTableGroupRecord().isSingleTableGroup()) {
                    sqlCreateTableStatement.setSingle(true);
                }

                changed = true;
            }
        }

        for (SQLTableElement element : sqlCreateTableStatement.getTableElementList()) {
            if (element instanceof MySqlTableIndex) {
                changed |= process4MysqlTableIndex(schemaName, tableName, (MySqlTableIndex) element,
                    forCheck, tgGroups);
            } else if (element instanceof MySqlKey) {
                if (!(element instanceof MySqlPrimaryKey)) {
                    if (element instanceof MySqlUnique) {
                        changed |= process4MysqlUnique(schemaName, tableName, (MySqlUnique) element,
                            forCheck, tgGroups);
                    } else {
                        changed |= process4MysqlKey(schemaName, tableName, (MySqlKey) element,
                            forCheck, tgGroups);
                    }
                }
            }
        }

        return changed;
    }

    private static boolean process4AlterTable(String schemaName, String tableName,
                                              SQLAlterTableStatement alterTableStatement, boolean forCheck,
                                              Set<String> tgGroups) {

        if (alterTableStatement.getAlterIndexName() != null) {
            return process4AlterIndex(schemaName, tableName, alterTableStatement, forCheck, tgGroups);
        } else {
            return process4AlterItems(schemaName, tableName, alterTableStatement, forCheck, tgGroups);
        }
    }

    private static boolean process4AlterItems(String schemaName, String tableName,
                                              SQLAlterTableStatement alterTableStatement, boolean forCheck,
                                              Set<String> tgGroups) {
        boolean changed = false;

        List<SQLAlterTableItem> items = alterTableStatement.getItems();
        if (items != null && !items.isEmpty()) {
            for (SQLAlterTableItem item : items) {
                if (item instanceof SQLAlterTableAddIndex) {
                    //"add key" or "add index" or "add global index"
                    SQLAlterTableAddIndex alterTableAddIndex = (SQLAlterTableAddIndex) item;
                    changed |= process4AlterTableAddIndex(schemaName, tableName, alterTableAddIndex,
                        forCheck, tgGroups);
                } else if (item instanceof SQLAlterTableAddConstraint) {
                    // "add unique key" or "add unique index" or "add unique global index"
                    SQLAlterTableAddConstraint alterTableAddConstraint = (SQLAlterTableAddConstraint) item;
                    SQLConstraint sqlConstraint = alterTableAddConstraint.getConstraint();
                    if (sqlConstraint instanceof MySqlUnique) {
                        changed |= process4MysqlUnique(schemaName, tableName, (MySqlUnique) sqlConstraint,
                            forCheck, tgGroups);
                    } else if (sqlConstraint instanceof MySqlTableIndex) {
                        changed |= process4MysqlTableIndex(
                            schemaName, tableName, (MySqlTableIndex) sqlConstraint, forCheck, tgGroups);
                    }
                } else if (item instanceof MySqlAlterTableModifyColumn) {
                    MySqlAlterTableModifyColumn modifyColumn = (MySqlAlterTableModifyColumn) item;
                    changed |= process4ModifyColumn(schemaName, tableName, alterTableStatement, modifyColumn,
                        forCheck, tgGroups);
                } else if (isAlterTableWithPartition(item)) {
                    changed |= process4Repartition(schemaName, tableName, alterTableStatement, forCheck, tgGroups);
                }
            }
        }

        if (alterTableStatement.getPartition() != null) {
            changed |= process4Repartition(schemaName, tableName, alterTableStatement, forCheck, tgGroups);
        }

        return changed;
    }

    private static boolean isAlterTableWithPartition(SQLAlterTableItem item) {
        return (item instanceof SQLAlterTableGroupItem && !(item instanceof SQLAlterTableTruncatePartition))
            || item instanceof DrdsAlterTableSingle
            || item instanceof DrdsAlterTableBroadcast;
    }

    private static boolean process4Repartition(String schemaName, String tableName,
                                               SQLAlterTableStatement alterTableStatement, boolean forCheck,
                                               Set<String> tgGroups) {
        TableGroupConfig tableGroupConfig =
            tableGroupConfigProvider.getTableGroupConfig(schemaName, buildQueryTableName(tableName), true);
        if (forCheck || tableGroupConfig == null) {
            tableGroupConfig =
                tableGroupConfigProvider.getTableGroupConfig(schemaName, buildQueryTableName(tableName), false);
        }

        if (tableGroupConfig != null && !tableGroupConfig.isManuallyCreated()) {
            if (forCheck) {
                Assert.assertNotNull(alterTableStatement.getTargetImplicitTableGroup());
                forceCheckTgName(alterTableStatement.getTargetImplicitTableGroup().getSimpleName(),
                    tableGroupConfig.getTableGroupRecord().getTg_name());
                tgGroups.add(alterTableStatement.getTargetImplicitTableGroup().getSimpleName());
            } else {
                tryCheckTgName(alterTableStatement.getTargetImplicitTableGroup(), tableGroupConfig);
                alterTableStatement.setTargetImplicitTableGroup(
                    new SQLIdentifierExpr(tableGroupConfig.getTableGroupRecord().getTg_name()));
                return true;
            }
        }
        return false;
    }

    private static boolean process4CreateIndex(String schemaName, String tableName,
                                               SQLCreateIndexStatement createIndexStatement, boolean forCheck,
                                               Set<String> tgGroups) {
        String indexName = SQLUtils.normalize(createIndexStatement.getName().getSimpleName());
        TableGroupConfig tableGroupConfig =
            tableGroupConfigProvider.getTableGroupConfig(schemaName, buildQueryTableName(tableName), indexName, false);

        if (tableGroupConfig != null && !tableGroupConfig.isManuallyCreated()) {
            if (forCheck) {
                Assert.assertNotNull(createIndexStatement.getTableGroup());
                forceCheckTgName(createIndexStatement.getTableGroup().getSimpleName(),
                    tableGroupConfig.getTableGroupRecord().getTg_name());
                Assert.assertTrue(createIndexStatement.isWithImplicitTablegroup());
                tgGroups.add(createIndexStatement.getTableGroup().getSimpleName());
            } else {
                tryCheckTgName(createIndexStatement.getTableGroup(), tableGroupConfig);
                createIndexStatement.setTableGroup(
                    new SQLIdentifierExpr(tableGroupConfig.getTableGroupRecord().getTg_name()));
                createIndexStatement.setWithImplicitTablegroup(true);
                return true;
            }
        }

        return false;
    }

    private static boolean process4AlterIndex(String schemaName, String tableName,
                                              SQLAlterTableStatement sqlAlterTableStatement, boolean forCheck,
                                              Set<String> tgGroups) {
        String indexName = SQLUtils.normalize(sqlAlterTableStatement.getAlterIndexName().getSimpleName());
        TableGroupConfig tableGroupConfig = tableGroupConfigProvider.getTableGroupConfig(
            schemaName, buildQueryTableName(tableName), indexName, true);
        if (forCheck || tableGroupConfig == null) {
            tableGroupConfig = tableGroupConfigProvider.getTableGroupConfig(
                schemaName, buildQueryTableName(tableName), indexName, false);
        }

        if (tableGroupConfig != null && !tableGroupConfig.isManuallyCreated()) {
            if (forCheck) {
                Assert.assertNotNull(sqlAlterTableStatement.getTargetImplicitTableGroup());
                forceCheckTgName(sqlAlterTableStatement.getTargetImplicitTableGroup().getSimpleName(),
                    tableGroupConfig.getTableGroupRecord().getTg_name());
                tgGroups.add(sqlAlterTableStatement.getTargetImplicitTableGroup().getSimpleName());
            } else {
                tryCheckTgName(sqlAlterTableStatement.getTargetImplicitTableGroup(), tableGroupConfig);
                sqlAlterTableStatement.setTargetImplicitTableGroup(
                    new SQLIdentifierExpr(tableGroupConfig.getTableGroupRecord().getTg_name()));
                return true;
            }
        }

        return false;
    }

    private static boolean process4MysqlUnique(String schemaName, String tableName, MySqlUnique mySqlUnique,
                                               boolean forCheck, Set<String> tgGroups) {
        boolean changed = false;
        TableGroupConfig config =
            tableGroupConfigProvider.getTableGroupConfig(schemaName, buildQueryTableName(tableName),
                SQLUtils.normalize(mySqlUnique.getName().getSimpleName()), false);

        if (config != null && !config.isManuallyCreated()) {
            if (forCheck) {
                Assert.assertNotNull(mySqlUnique.getTableGroup());
                forceCheckTgName(mySqlUnique.getTableGroup().getSimpleName(),
                    config.getTableGroupRecord().getTg_name());
                Assert.assertTrue(mySqlUnique.isWithImplicitTablegroup());
                tgGroups.add(mySqlUnique.getTableGroup().getSimpleName());
            } else {
                tryCheckTgName(mySqlUnique.getTableGroup(), config);
                mySqlUnique.setTableGroup(new SQLIdentifierExpr(config.getTableGroupRecord().getTg_name()));
                mySqlUnique.setWithImplicitTablegroup(true);
                changed = true;
            }
        }
        return changed;
    }

    private static boolean process4MysqlTableIndex(String schemaName, String tableName, MySqlTableIndex tableIndex,
                                                   boolean forCheck, Set<String> tgGroups) {
        boolean changed = false;
        TableGroupConfig config =
            tableGroupConfigProvider.getTableGroupConfig(schemaName, buildQueryTableName(tableName),
                SQLUtils.normalize(tableIndex.getName().getSimpleName()), false);

        if (config != null && !config.isManuallyCreated()) {
            if (forCheck) {
                Assert.assertNotNull(tableIndex.getTableGroup());
                forceCheckTgName(tableIndex.getTableGroup().getSimpleName(),
                    config.getTableGroupRecord().getTg_name());
                Assert.assertTrue(tableIndex.isWithImplicitTablegroup());
                tgGroups.add(tableIndex.getTableGroup().getSimpleName());
            } else {
                tryCheckTgName(tableIndex.getTableGroup(), config);
                tableIndex.setTableGroup(new SQLIdentifierExpr(config.getTableGroupRecord().getTg_name()));
                tableIndex.setWithImplicitTablegroup(true);
                changed = true;
            }
        }
        return changed;
    }

    private static boolean process4MysqlKey(String schemaName, String tableName, MySqlKey mySqlKey, boolean forCheck,
                                            Set<String> tgGroups) {
        boolean changed = false;
        TableGroupConfig config =
            tableGroupConfigProvider.getTableGroupConfig(schemaName, buildQueryTableName(tableName),
                SQLUtils.normalize(mySqlKey.getName().getSimpleName()), false);

        if (config != null && !config.isManuallyCreated()) {
            if (forCheck) {
                Assert.assertNotNull(mySqlKey.getIndexDefinition().getTableGroup());
                forceCheckTgName(mySqlKey.getIndexDefinition().getTableGroup().getSimpleName(),
                    config.getTableGroupRecord().getTg_name());
                Assert.assertTrue(mySqlKey.getIndexDefinition().isWithImplicitTablegroup());
                tgGroups.add(mySqlKey.getIndexDefinition().getTableGroup().getSimpleName());
            } else {
                tryCheckTgName(mySqlKey.getIndexDefinition().getTableGroup(), config);
                mySqlKey.getIndexDefinition().setTableGroup(
                    new SQLIdentifierExpr(config.getTableGroupRecord().getTg_name()));
                mySqlKey.getIndexDefinition().setWithImplicitTablegroup(true);
                changed = true;
            }
        }
        return changed;
    }

    private static boolean process4AlterTableAddIndex(String schemaName, String tableName,
                                                      SQLAlterTableAddIndex alterTableAddIndex, boolean forCheck,
                                                      Set<String> tgGroups) {
        TableGroupConfig config =
            tableGroupConfigProvider.getTableGroupConfig(schemaName, buildQueryTableName(tableName),
                SQLUtils.normalize(alterTableAddIndex.getName().getSimpleName()), false);

        if (config != null && !config.isManuallyCreated()) {
            if (forCheck) {
                Assert.assertNotNull(alterTableAddIndex.getTableGroup());
                forceCheckTgName(alterTableAddIndex.getTableGroup().getSimpleName(),
                    config.getTableGroupRecord().getTg_name());
                Assert.assertTrue(alterTableAddIndex.isWithImplicitTablegroup());
                tgGroups.add(alterTableAddIndex.getTableGroup().getSimpleName());
            } else {
                tryCheckTgName(alterTableAddIndex.getTableGroup(), config);
                alterTableAddIndex.setTableGroup(
                    new SQLIdentifierExpr(config.getTableGroupRecord().getTg_name()));
                alterTableAddIndex.setWithImplicitTablegroup(true);
                return true;
            }
        }
        return false;
    }

    private static boolean process4ModifyColumn(String schemaName, String tableName,
                                                SQLAlterTableStatement alterTableStatement,
                                                MySqlAlterTableModifyColumn modifyColumn,
                                                boolean forCheck, Set<String> tgGroups) {
        boolean changed = false;

        String modifyColumnName = SQLUtils.normalize(
            modifyColumn.getNewColumnDefinition().getColumnName()).toLowerCase();
        PartitionColumnInfo partitionColumnInfo =
            tableGroupConfigProvider.getPartitionColumnInfo(schemaName, tableName);

        for (Map.Entry<String, Set<String>> i : partitionColumnInfo.gsiPartitionColumns.entrySet()) {
            if (i.getValue().contains(modifyColumnName.toLowerCase())) {
                TableGroupConfig config = tableGroupConfigProvider.getTableGroupConfig(
                    schemaName, tableName, buildQueryGsiName(i.getKey()), false);
                if (config != null && !config.isManuallyCreated()) {
                    if (forCheck) {
                        Pair<SQLName, SQLName> pair = alterTableStatement.getIndexTableGroupPair().stream()
                            .filter(p -> StringUtils.equalsIgnoreCase(p.getKey().getSimpleName(), i.getKey()))
                            .findFirst().orElse(null);
                        Assert.assertNotNull(pair);
                        forceCheckTgName(pair.getValue().getSimpleName(), config.getTableGroupRecord().getTg_name());
                        tgGroups.add(pair.getValue().getSimpleName());
                    } else {
                        if (alterTableStatement.getIndexTableGroupPair().stream()
                            .noneMatch(p -> StringUtils.equalsIgnoreCase(p.getKey().getSimpleName(), i.getKey()))) {
                            alterTableStatement.addIndexTableGroupPair(new SQLIdentifierExpr(i.getKey()),
                                new SQLIdentifierExpr(config.getTableGroupRecord().tg_name));
                        }
                        changed = true;
                    }
                }
            }
        }

        // 索引所属的表组发生变更，可能也会导致主表的表组发生变更，即使modify column不是主表的分区键
        // 示例如下：第三条sql，会导致主表和索引各自新建一个table group
        // create table if not exists `t_order` (
        //  `order_id` varchar(20) DEFAULT NULL,
        //  `seller_id` varchar(20) DEFAULT NULL,
        //  GLOBAL INDEX seller_id(`seller_id`) PARTITION BY KEY (seller_id)
        // ) DEFAULT CHARACTER SET = utf8mb4 DEFAULT COLLATE = utf8mb4_general_ci
        // PARTITION BY KEY (order_id);
        //
        // ALTER TABLE `t_order` MODIFY COLUMN seller_id varchar(30);
        // ALTER TABLE `t_order` MODIFY COLUMN seller_id varchar(30) CHARACTER SET utf8;
        if (partitionColumnInfo.tablePartitionColumns.contains(modifyColumnName) || changed) {
            TableGroupConfig tableGroupConfig = tableGroupConfigProvider.getTableGroupConfig(
                schemaName, buildQueryTableName(tableName), false);
            if (tableGroupConfig != null && !tableGroupConfig.isManuallyCreated()) {
                if (forCheck) {
                    Assert.assertNotNull(alterTableStatement.getTargetImplicitTableGroup());
                    forceCheckTgName(alterTableStatement.getTargetImplicitTableGroup().getSimpleName(),
                        tableGroupConfig.getTableGroupRecord().getTg_name());
                    tgGroups.add(alterTableStatement.getTargetImplicitTableGroup().getSimpleName());
                } else {
                    tryCheckTgName(alterTableStatement.getTargetImplicitTableGroup(), tableGroupConfig);
                    alterTableStatement.setTargetImplicitTableGroup(
                        new SQLIdentifierExpr(tableGroupConfig.getTableGroupRecord().getTg_name()));
                    changed = true;
                }
            }
        }

        return changed;
    }

    private static void removeImplicitTgForCreateTable(MySqlCreateTableStatement createTableStatement) {
        createTableStatement.setTableGroup(null);
        createTableStatement.setWithImplicitTablegroup(false);

        for (SQLTableElement element : createTableStatement.getTableElementList()) {
            if (element instanceof MySqlTableIndex) {
                MySqlTableIndex tableIndex = (MySqlTableIndex) element;
                tableIndex.setTableGroup(null);
                tableIndex.setWithImplicitTablegroup(false);
            } else if (element instanceof MySqlKey) {
                if (!(element instanceof MySqlPrimaryKey)) {
                    if (element instanceof MySqlUnique) {
                        MySqlUnique mySqlUnique = (MySqlUnique) element;
                        mySqlUnique.setTableGroup(null);
                        mySqlUnique.setWithImplicitTablegroup(false);
                    } else {
                        MySqlKey mySqlKey = (MySqlKey) element;
                        mySqlKey.getIndexDefinition().setTableGroup(null);
                        mySqlKey.getIndexDefinition().setWithImplicitTablegroup(false);
                    }
                }
            }
        }
    }

    private static void removeImplicitTgForAlterTable(SQLAlterTableStatement alterTableStatement) {
        alterTableStatement.setTargetImplicitTableGroup(null);

        List<SQLAlterTableItem> items = alterTableStatement.getItems();
        if (items != null && !items.isEmpty()) {
            for (SQLAlterTableItem item : items) {
                if (item instanceof SQLAlterTableAddIndex) {
                    SQLAlterTableAddIndex alterTableAddIndex = (SQLAlterTableAddIndex) item;
                    alterTableAddIndex.setTableGroup(null);
                    alterTableAddIndex.setWithImplicitTablegroup(false);
                } else if (item instanceof SQLAlterTableAddConstraint) {
                    SQLAlterTableAddConstraint alterTableAddConstraint = (SQLAlterTableAddConstraint) item;
                    SQLConstraint sqlConstraint = alterTableAddConstraint.getConstraint();
                    if (sqlConstraint instanceof MySqlUnique) {
                        MySqlUnique mySqlUnique = (MySqlUnique) sqlConstraint;
                        mySqlUnique.setTableGroup(null);
                        mySqlUnique.setWithImplicitTablegroup(false);
                    } else if (sqlConstraint instanceof MySqlTableIndex) {
                        MySqlTableIndex tableIndex = (MySqlTableIndex) sqlConstraint;
                        tableIndex.setTableGroup(null);
                        tableIndex.setWithImplicitTablegroup(false);
                    }
                }
            }
        }
    }

    private static void forceCheckTgName(String actualValue, String expectValue) {
        if (tableGroupConfigProvider.isCheckTgNameValue()) {
            Assert.assertTrue(StringUtils.equalsIgnoreCase(actualValue, expectValue),
                String.format("force check tg_name failed, expect values is %s, actual values is %s.", expectValue,
                    actualValue));
        }
    }

    private static void tryCheckTgName(SQLName expectValue, TableGroupConfig actualValue) {
        if (expectValue != null && StringUtils.isNotBlank(expectValue.getSimpleName())) {
            String expectName = SQLUtils.normalize(expectValue.getSimpleName());
            String actualName = actualValue.getTableGroupRecord().getTg_name();
            Assert.assertTrue(StringUtils.equalsIgnoreCase(actualName, expectName),
                String.format("try check tg_name failed, expect tg name is %s, actual tg name is %s.",
                    expectName, actualName));
        }
    }

    private static String buildQueryTableName(String tableName) {
        if (exchangeNamesMapping.get() != null) {
            return exchangeNamesMapping.get().getOrDefault(tableName, tableName);
        }
        return tableName;
    }

    private static String buildQueryGsiName(String gsiName) {
        if (exchangeNamesMapping.get() != null) {
            if (exchangeNamesMapping.get().containsKey(gsiName)) {
                return exchangeNamesMapping.get().get(gsiName);
            } else {
                for (Map.Entry<String, String> entry : exchangeNamesMapping.get().entrySet()) {
                    if (entry.getKey().contains("_$")) {
                        String newKey = unwrapGsiName(entry.getKey());
                        if (StringUtils.equalsIgnoreCase(newKey, gsiName)) {
                            return entry.getValue();
                        }
                    }
                }
            }
        }
        return gsiName;
    }

    public interface TableGroupConfigProvider {
        boolean isNewPartitionDb(String schemaName);

        boolean isCheckTgNameValue();

        TableGroupConfig getTableGroupConfig(String schemaName, String tableName, boolean fromDelta);

        TableGroupConfig getTableGroupConfig(String schemaName, String tableName, String gsiName, boolean fromDelta);

        PartitionColumnInfo getPartitionColumnInfo(String schemaName, String tableName);
    }

    public static class DefaultTableGroupConfigProvider implements TableGroupConfigProvider {

        @Override
        public boolean isCheckTgNameValue() {
            return true;
        }

        @Override
        public boolean isNewPartitionDb(String schemaName) {
            return DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        }

        @Override
        public TableGroupConfig getTableGroupConfig(String schemaName, String tableName, boolean fromDelta) {
            TablePartitionConfig tbPartConf =
                TablePartitionConfigUtil.getTablePartitionConfig(schemaName, tableName, fromDelta);

            return tbPartConf == null ? null :
                TableGroupUtils.getTableGroupInfoByGroupId(tbPartConf.getTableConfig().groupId);
        }

        @Override
        public TableGroupConfig getTableGroupConfig(String schemaName, String tableName, String gsiName,
                                                    boolean fromDelta) {
            Long tableGroupId = TableGroupInfoManager.getTableGroupId(
                schemaName, tableName, gsiName, true, fromDelta);
            return tableGroupId == null ? null :
                TableGroupUtils.getTableGroupInfoByGroupId(tableGroupId);
        }

        @Override
        public PartitionColumnInfo getPartitionColumnInfo(String schemaName, String tableName) {
            Map<String, Set<String>> gsiPartitionColumns = new HashMap<>();
            Set<String> tablePartitionColumns = new HashSet<>();

            TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
            if (tableMeta.getGsiPublished() != null) {
                for (GsiMetaManager.GsiIndexMetaBean i : tableMeta.getGsiPublished().values()) {
                    final String gsiName = StringUtils.substringBeforeLast(i.indexName, "_$");
                    Set<String> gsiColumns = i.indexColumns.stream()
                        .map(c -> c.columnName.toLowerCase())
                        .collect(Collectors.toSet());
                    gsiPartitionColumns.put(gsiName, gsiColumns);
                }
            }
            if (tableMeta.getPartitionInfo() != null) {
                List<String> list = tableMeta.getPartitionInfo().getPartitionColumns();
                tablePartitionColumns = list.stream().map(StringUtils::lowerCase).collect(Collectors.toSet());
            }
            return new PartitionColumnInfo(schemaName, tableName, tablePartitionColumns, gsiPartitionColumns);
        }
    }

    @AllArgsConstructor
    @Data
    public static class PartitionColumnInfo {
        String schemaName;
        String tableName;
        Set<String> tablePartitionColumns = new HashSet<>();
        Map<String, Set<String>> gsiPartitionColumns = new HashMap<>();
    }
}
