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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.ArchiveMode;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.DdlVisibility;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.builder.CreatePartitionTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.CreateTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.CreatePartitionTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.CreatePartitionTableWithGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateTableSelectJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateTableWithGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.oss.CreatePartitionOssTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.InsertIntoTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.LogicalInsertTask;
import com.alibaba.polardbx.executor.ddl.job.validator.ColumnValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ConstraintValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ForeignKeyValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionTable;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateTable;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4InsertOverwrite;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.handler.LogicalShowCreateTableHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.DefaultExprUtil;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import groovy.sql.Sql;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.CreateTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.CreateTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.SqlSubPartitionBy;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.ERROR_TABLE_EXISTS;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_CREATE_SELECT_UPDATE;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_PARTITION_MANAGEMENT;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TABLE_ALREADY_EXISTS;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TABLE_ALREADY_EXISTS;
import static com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper.deSerializeTask;

public class LogicalCreateTableHandler extends LogicalCommonDdlHandler {

    public LogicalCreateTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalCreateTable logicalCreateTable = (LogicalCreateTable) logicalDdlPlan;

        SqlCreateTable sqlCreateTable = (SqlCreateTable) logicalCreateTable.relDdl.sqlNode;
        boolean flag = false;
        // create select 的逻辑
        if (sqlCreateTable.getQuery() != null || sqlCreateTable.getAsTableName() != null) {
            flag = true;

            logicalCreateTable = generatePlan(logicalCreateTable, executionContext);

            // Validate the plan on file storage first
            TableValidator.validateTableEngine(logicalCreateTable, executionContext);
            boolean returnImmediately = validatePlan(logicalCreateTable, executionContext);
            if (returnImmediately) {
                return new TransientDdlJob();
            }

        }

        sqlCreateTable = (SqlCreateTable) logicalCreateTable.relDdl.sqlNode;

        if (sqlCreateTable.getLikeTableName() != null) {
            final String sourceCreateTableSql = generateCreateTableSqlForLike(sqlCreateTable, executionContext);
            MySqlCreateTableStatement stmt =
                (MySqlCreateTableStatement) FastsqlUtils.parseSql(sourceCreateTableSql).get(0);
            stmt.getTableSource().setSimpleName(SqlIdentifier.surroundWithBacktick(logicalCreateTable.getTableName()));
            final String targetCreateTableSql = stmt.toString();
            final SqlCreateTable targetTableAst = (SqlCreateTable)
                new FastsqlParser().parse(targetCreateTableSql, executionContext).get(0);

            PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
            SqlCreateTable createTableLikeSqlAst = (SqlCreateTable)
                new FastsqlParser().parse(sourceCreateTableSql, executionContext).get(0);
            createTableLikeSqlAst.setSourceSql(targetTableAst.getSourceSql());

            // handle engine
            if (Engine.isFileStore(createTableLikeSqlAst.getEngine()) &&
                !executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_CREATE_TABLE_LIKE_FILE_STORE)) {
                throw GeneralUtil.nestedException(
                    "cannot create table like an file-store table, the engine of source table must be INNODB.");
            }

            // set engine if there is engine option in user sql
            Engine engine;
            if ((engine = sqlCreateTable.getEngine()) != null) {
                createTableLikeSqlAst.setEngine(engine);
            }

            // set archive mode if there is engine option in user sql
            ArchiveMode archiveMode;
            if ((archiveMode = sqlCreateTable.getArchiveMode()) != null) {
                createTableLikeSqlAst.setArchiveMode(archiveMode);
            }

            if (!Engine.isFileStore(engine) && archiveMode != null) {
                throw GeneralUtil.nestedException(
                    "cannot create table using ARCHIVE_MODE if the engine of target table is INNODB.");
            }

            SqlIdentifier sourceTableName = (SqlIdentifier) sqlCreateTable.getLikeTableName();
            String sourceSchema =
                sourceTableName.names.size() > 1 ? sourceTableName.names.get(0) : executionContext.getSchemaName();

            SqlIdentifier targetTableName = (SqlIdentifier) sqlCreateTable.getName();
            String targetSchema =
                targetTableName.names.size() > 1 ? targetTableName.names.get(0) : executionContext.getSchemaName();

            // The above statement conversion probably lost locality info,
            // so we should find it from the partition info of the source table.
            if (DbInfoManager.getInstance().isNewPartitionDb(sourceSchema)
                && DbInfoManager.getInstance().isNewPartitionDb(targetSchema)
                && TStringUtil.isEmpty(createTableLikeSqlAst.getLocality())) {
                PartitionInfo sourcePartInfo = OptimizerContext.getContext(sourceSchema).getPartitionInfoManager()
                    .getPartitionInfo(sourceTableName.getLastName());
                if (sourcePartInfo == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("Could not find partition info for %s", sourceTableName));
                }
                createTableLikeSqlAst.setLocality(sourcePartInfo.getLocality());
            }

            createTableLikeSqlAst.setTargetTable(sqlCreateTable.getTargetTable());
            if (!DbInfoManager.getInstance().isNewPartitionDb(logicalDdlPlan.getSchemaName())) {
                createTableLikeSqlAst.setGlobalKeys(null);
                createTableLikeSqlAst.setGlobalUniqueKeys(null);
            }

            if (sqlCreateTable.shouldLoad() || sqlCreateTable.shouldBind()) {
                if (sqlCreateTable.getLikeTableName() instanceof SqlIdentifier) {
                    if (((SqlIdentifier) sqlCreateTable.getLikeTableName()).names.size() == 2) {
                        createTableLikeSqlAst.setLoadTableSchema(
                            ((SqlIdentifier) sqlCreateTable.getLikeTableName()).getComponent(0).getLastName());
                        createTableLikeSqlAst.setLoadTableName(
                            ((SqlIdentifier) sqlCreateTable.getLikeTableName()).getComponent(1).getLastName());
                    } else if (((SqlIdentifier) sqlCreateTable.getLikeTableName()).names.size() == 1) {
                        createTableLikeSqlAst.setLoadTableName(
                            ((SqlIdentifier) sqlCreateTable.getLikeTableName()).getComponent(0).getLastName());
                    }
                }
            }

            ExecutionPlan createTableLikeSqlPlan = Planner.getInstance().getPlan(createTableLikeSqlAst, plannerContext);
            LogicalCreateTable logicalCreateTableLikeRelNode = (LogicalCreateTable) createTableLikeSqlPlan.getPlan();

            if (logicalCreateTable.getSqlCreate().isSelect()) {
                logicalCreateTableLikeRelNode.getSqlCreate().setSelect(true);
            }
            if (logicalCreateTable.getSqlSelect() != null) {
                logicalCreateTableLikeRelNode.setSqlSelect(logicalCreateTable.getSqlSelect());
            }
            if (logicalCreateTable.isIgnore()) {
                logicalCreateTableLikeRelNode.setIgnore(logicalCreateTable.isIgnore());
            }
            if (logicalCreateTable.isReplace()) {
                logicalCreateTableLikeRelNode.setReplace(logicalCreateTable.isReplace());
            }

            logicalCreateTable = logicalCreateTableLikeRelNode;
            // Validate the plan on file storage first
            TableValidator.validateTableEngine(logicalCreateTable, executionContext);
            boolean returnImmediately = validatePlan(logicalCreateTable, executionContext);
            if (returnImmediately) {
                return new TransientDdlJob();
            }
        }

        // Check unique index
        Set<String> uniqueColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> generatedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        if (sqlCreateTable.getColDefs() != null) {
            for (Pair<SqlIdentifier, SqlColumnDeclaration> colDef : sqlCreateTable.getColDefs()) {
                String columnName = colDef.getKey().getLastName();
                if (colDef.getValue().isGeneratedAlways() && !colDef.getValue().isGeneratedAlwaysLogical()) {
                    SqlCall expr = colDef.getValue().getGeneratedAlwaysExpr();
                    GeneratedColumnUtil.validateGeneratedColumnExpr(expr);
                    generatedColumns.add(columnName);
                }
                if (colDef.getValue().getSpecialIndex() != null) {
                    uniqueColumns.add(columnName);
                }
                if (colDef.getValue().getDefaultExpr() != null) {
                    if (!DefaultExprUtil.supportDataType(colDef.getValue())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Do not support data type of column `%s` with default expression.",
                                colDef.getKey()));
                    }
                    SqlCall expr = colDef.getValue().getDefaultExpr();
                    DefaultExprUtil.validateColumnExpr(expr);
                }
            }
        }

        if (sqlCreateTable.getUniqueKeys() != null) {
            for (Pair<SqlIdentifier, SqlIndexDefinition> uniqueKey : sqlCreateTable.getUniqueKeys()) {
                SqlIndexDefinition sqlIndexDefinition = uniqueKey.getValue();
                for (SqlIndexColumnName column : sqlIndexDefinition.getColumns()) {
                    uniqueColumns.add(column.getColumnNameStr());
                }
            }
        }

        if (sqlCreateTable.getGlobalUniqueKeys() != null) {
            for (Pair<SqlIdentifier, SqlIndexDefinition> uniqueKey : sqlCreateTable.getGlobalUniqueKeys()) {
                SqlIndexDefinition sqlIndexDefinition = uniqueKey.getValue();
                for (SqlIndexColumnName column : sqlIndexDefinition.getColumns()) {
                    uniqueColumns.add(column.getColumnNameStr());
                }
            }
        }

        if (sqlCreateTable.getPrimaryKey() != null) {
            for (SqlIndexColumnName column : sqlCreateTable.getPrimaryKey().getColumns()) {
                uniqueColumns.add(column.getColumnNameStr());
            }
        }

        if (generatedColumns.stream().anyMatch(uniqueColumns::contains) && !executionContext.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_UNIQUE_KEY_ON_GEN_COL)) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "create unique index on VIRTUAL/STORED generated column is not enabled");
        }
        if (sqlCreateTable.getTableGroupName() != null) {
            expandTableGroupDefinition(logicalCreateTable.relDdl, logicalCreateTable.getSchemaName(), executionContext);
        }
        logicalCreateTable.prepareData(executionContext);
        if (flag) {
            String createTableSql = logicalCreateTable.getNativeSql();
            String insertIntoSql = logicalCreateTable.getCreateTablePreparedData().getSelectSql();
            return new CreateTableSelectJobFactory(logicalCreateTable.getSchemaName(),
                logicalCreateTable.getTableName(), executionContext, createTableSql, insertIntoSql).create();
        }
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(logicalCreateTable.getSchemaName());
        if (!isNewPartDb) {
            if (logicalCreateTable.isWithGsi()) {
                return buildCreateTableWithGsiJob(logicalCreateTable, executionContext);
            } else {
                return buildCreateTableJob(logicalCreateTable, executionContext);
            }
        } else {
            if (logicalCreateTable.isWithGsi()) {
                return buildCreatePartitionTableWithGsiJob(logicalCreateTable, executionContext);
            } else {
                return buildCreatePartitionTableJob(logicalCreateTable, executionContext);
            }
        }
    }

    public int getIndex(List<Pair<SqlIdentifier, SqlColumnDeclaration>> createCols, String colName) {
        for (int i = 0; i < createCols.size(); ++i) {
            if (createCols.get(i).getKey().getLastName().equalsIgnoreCase(colName)) {
                return i;
            }
        }
        return -1;
    }

    private void expandTableGroupDefinition(DDL createTable, String schemaName, ExecutionContext ec) {

        SqlCreateTable sqlCreateTable = (SqlCreateTable) createTable.sqlNode;
        String tableGroupName = sqlCreateTable.getTableGroupName() == null ? null :
            ((SqlIdentifier) sqlCreateTable.getTableGroupName()).getLastName();
        if (StringUtils.isEmpty(tableGroupName)) {
            return;
        }

        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        if (tableGroupConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS, tableGroupName);
        }
        String partitionDef = tableGroupConfig.getPreDefinePartitionInfo();
        if (StringUtils.isEmpty(partitionDef)) {
            return;
        }
        SqlPartitionBy tableSqlPartitionBy = (SqlPartitionBy) sqlCreateTable.getSqlPartition();
        SQLPartitionBy fastSqlPartitionBy = FastsqlUtils.parsePartitionBy(partitionDef, true);
        FastSqlToCalciteNodeVisitor visitor =
            new FastSqlToCalciteNodeVisitor(new ContextParameters(false), ec);
        fastSqlPartitionBy.accept(visitor);
        SqlPartitionBy tableGroupSqlPartitionBy = (SqlPartitionBy) visitor.getSqlNode();

        validateSqlPartitionBy(tableGroupSqlPartitionBy, sqlCreateTable);

        tableGroupSqlPartitionBy.getColumns().clear();
        tableGroupSqlPartitionBy.getColumns().addAll(tableSqlPartitionBy.getColumns());

        SqlSubPartitionBy sqlSubPartitionBy = tableGroupSqlPartitionBy.getSubPartitionBy();
        if (sqlSubPartitionBy != null) {
            sqlSubPartitionBy.getColumns().clear();
            sqlSubPartitionBy.getColumns().addAll(tableSqlPartitionBy.getSubPartitionBy().getColumns());
        }
        sqlCreateTable.setSqlPartition(tableGroupSqlPartitionBy);

        SqlConverter sqlConverter = SqlConverter.getInstance(schemaName, ec);
        PlannerContext plannerContext = PlannerContext.getPlannerContext(createTable.getCluster());

        Map<SqlNode, RexNode> partRexInfoCtx = sqlConverter.convertPartition(tableGroupSqlPartitionBy, plannerContext);

        ((CreateTable) (createTable)).getPartBoundExprInfo().putAll(partRexInfoCtx);
    }

    private void validateSqlPartitionBy(SqlPartitionBy tableGroupSqlPartitionBy, SqlCreateTable sqlCreateTable) {
        SqlPartitionBy tableSqlPartitionBy = (SqlPartitionBy) sqlCreateTable.getSqlPartition();
        if (tableSqlPartitionBy == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_PARTITION_STRATEGY);
        }
        if (Objects.requireNonNull(tableGroupSqlPartitionBy).getClass() != Objects.requireNonNull(tableSqlPartitionBy)
            .getClass()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_STRATEGY_IS_NOT_EQUAL);
        }
        if (GeneralUtil.isNotEmpty(tableSqlPartitionBy.getPartitions())) {
            throw new TddlRuntimeException(ErrorCode.ERR_REDUNDANCY_PARTITION_DEFINITION);
        }
        if (tableSqlPartitionBy.getPartitionsCount() != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_REDUNDANCY_PARTITION_DEFINITION);
        }
        List<SqlNode> partColumns = new ArrayList<>(tableSqlPartitionBy.getColumns().size());
        List<SqlNode> subPartColumns = new ArrayList<>();
        for (SqlNode sqlNode : tableSqlPartitionBy.getColumns()) {
            if (sqlNode instanceof SqlCall) {
                partColumns.add(sqlNode);
            } else {
                String colName = SQLUtils.normalizeNoTrim(((SqlIdentifier) sqlNode).getLastName());
                for (Pair<SqlIdentifier, SqlColumnDeclaration> pair : sqlCreateTable.getColDefs()) {
                    String curColName = SQLUtils.normalizeNoTrim(pair.left.getLastName());
                    if (colName.equalsIgnoreCase(curColName)) {
                        partColumns.add(pair.right);
                        break;
                    }
                }
            }
        }

        tableSqlPartitionBy.getColumns();
        sqlCreateTable.getColDefs();
        SqlSubPartitionBy tgSqlSubPartitionBy = tableGroupSqlPartitionBy.getSubPartitionBy();
        SqlSubPartitionBy tbSqlSubPartitionBy = tableSqlPartitionBy.getSubPartitionBy();
        if (tgSqlSubPartitionBy != null && tbSqlSubPartitionBy == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_SUBPARTITION_STRATEGY);
        } else if (tgSqlSubPartitionBy == null && tbSqlSubPartitionBy != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_SUBPARTITION_STRATEGY_NOT_EXIST);
        } else if (tgSqlSubPartitionBy != null && tbSqlSubPartitionBy != null) {
            if (tgSqlSubPartitionBy.getClass() != tbSqlSubPartitionBy.getClass()) {
                throw new TddlRuntimeException(ErrorCode.ERR_SUBPARTITION_STRATEGY_IS_NOT_EQUAL);
            }
            if (GeneralUtil.isNotEmpty(tbSqlSubPartitionBy.getSubPartitions())) {
                throw new TddlRuntimeException(ErrorCode.ERR_REDUNDANCY_SUBPARTITION_DEFINITION);
            }
            if (tbSqlSubPartitionBy.getSubPartitionsCount() != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_REDUNDANCY_SUBPARTITION_DEFINITION);
            }
            for (SqlNode sqlNode : tbSqlSubPartitionBy.getColumns()) {
                if (sqlNode instanceof SqlCall) {
                    subPartColumns.add(sqlNode);
                } else {
                    String colName = SQLUtils.normalizeNoTrim(((SqlIdentifier) sqlNode).getLastName());
                    for (Pair<SqlIdentifier, SqlColumnDeclaration> pair : sqlCreateTable.getColDefs()) {
                        String curColName = SQLUtils.normalizeNoTrim(pair.left.getLastName());
                        if (colName.equalsIgnoreCase(curColName)) {
                            subPartColumns.add(pair.right);
                            break;
                        }
                    }
                }
            }
        }
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlCreateTable sqlCreateTable = (SqlCreateTable) logicalDdlPlan.getNativeSqlNode();
        final String schemaName = logicalDdlPlan.getSchemaName();
        final String logicalTableName = logicalDdlPlan.getTableName();
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);

        if (isNewPart) {
            if (sqlCreateTable.isBroadCast()) {
                String tableGroupName = sqlCreateTable.getTableGroupName() == null ? null :
                    ((SqlIdentifier) sqlCreateTable.getTableGroupName()).getLastName();
                if (tableGroupName != null && !TableGroupNameUtil.BROADCAST_TG_NAME_TEMPLATE
                    .equalsIgnoreCase(tableGroupName)) {
                    throw new TddlRuntimeException(ERR_PARTITION_MANAGEMENT,
                        "can't set the broadcast table's tablegroup explicitly");
                }
            }
        }
        if (sqlCreateTable.isSelect() == true) {
            // 处理 create select 语法
            SqlSelect select = ((LogicalCreateTable) logicalDdlPlan).getSqlSelect();
            if (select.getLockMode() == SqlSelect.LockMode.EXCLUSIVE_LOCK) {
                throw new TddlRuntimeException(ERR_CREATE_SELECT_UPDATE,
                    "Have an error in your SQL syntax, select part cannot have for update part");
            }
        }
        if (sqlCreateTable.getLikeTableName() != null) {
            if (((SqlIdentifier) sqlCreateTable.getLikeTableName()).names.size() == 1) {
                final String likeTableName = ((SqlIdentifier) sqlCreateTable.getLikeTableName()).getLastName();
                if (TStringUtil.equalsIgnoreCase(logicalTableName, likeTableName)) {
                    throw new TddlNestableRuntimeException(
                        String.format("Not unique table/alias: '%s'", logicalTableName));
                }

                if (!TableValidator.checkIfTableExists(schemaName, likeTableName)) {
                    throw new TddlNestableRuntimeException(
                        String.format("Table '%s.%s' doesn't exist", schemaName, likeTableName));
                }
            } else if (((SqlIdentifier) sqlCreateTable.getLikeTableName()).names.size() == 2) {
                final String likeTableSchema =
                    ((SqlIdentifier) sqlCreateTable.getLikeTableName()).getComponent(0).getLastName();
                final String likeTableName =
                    ((SqlIdentifier) sqlCreateTable.getLikeTableName()).getComponent(1).getLastName();
                if (TStringUtil.equalsIgnoreCase(schemaName, likeTableSchema) &&
                    TStringUtil.equalsIgnoreCase(logicalTableName, likeTableName)) {
                    throw new TddlNestableRuntimeException(
                        String.format("Not unique table/alias: '%s'", logicalTableName));
                }

                if (!TableValidator.checkIfTableExists(likeTableSchema, likeTableName)) {
                    throw new TddlNestableRuntimeException(
                        String.format("Table '%s.%s' doesn't exist", likeTableSchema, likeTableName));
                }
            } else {
                throw new AssertionError("create table like with wrong argument");
            }
            return false;
        }

        TableValidator.validateTableInfo(logicalDdlPlan.getSchemaName(), logicalDdlPlan.getTableName(),
            sqlCreateTable, executionContext.getParamManager());

        boolean tableExists = TableValidator.checkIfTableExists(schemaName, logicalTableName);
        if (tableExists && sqlCreateTable.isIfNotExists()) {
            DdlContext ddlContext = executionContext.getDdlContext();
            CdcManagerHelper.getInstance().notifyDdlNew(schemaName, logicalTableName, SqlKind.CREATE_TABLE.name(),
                ddlContext.getDdlStmt(), ddlContext.getDdlType(), null, null,
                DdlVisibility.Public, buildExtendParameter(executionContext));

            // Prompt "show warning" only.
            DdlHelper.storeFailedMessage(schemaName, ERROR_TABLE_EXISTS,
                " Table '" + logicalTableName + "' already exists", executionContext);
            executionContext.getDdlContext().setUsingWarning(true);
            return true;
        } else if (tableExists) {
            throw new TddlRuntimeException(ERR_TABLE_ALREADY_EXISTS, logicalTableName);
        }

        ForeignKeyValidator.validateFkConstraints(sqlCreateTable, logicalDdlPlan.getSchemaName(),
            logicalDdlPlan.getTableName(), executionContext);

        ColumnValidator.validateColumnLimits(sqlCreateTable);

        IndexValidator.validateIndexNameLengths(sqlCreateTable);

        ConstraintValidator.validateConstraintLimits(sqlCreateTable);

        return false;
    }

    protected DdlJob buildCreateTableJob(LogicalCreateTable logicalCreateTable, ExecutionContext executionContext) {
        CreateTablePreparedData createTablePreparedData = logicalCreateTable.getCreateTablePreparedData();

        DdlPhyPlanBuilder createTableBuilder =
            new CreateTableBuilder(logicalCreateTable.relDdl, createTablePreparedData, executionContext).build();
        PhysicalPlanData physicalPlanData = createTableBuilder.genPhysicalPlanData();

        CreateTableJobFactory ret = new CreateTableJobFactory(
            createTablePreparedData.isAutoPartition(),
            createTablePreparedData.isTimestampColumnDefault(),
            createTablePreparedData.getSpecialDefaultValues(),
            createTablePreparedData.getSpecialDefaultValueFlags(),
            createTablePreparedData.getAddedForeignKeys(),
            physicalPlanData,
            executionContext
        );
        logicalCreateTable.setAffectedRows(ret.getAffectRows());
        if (createTablePreparedData.getSelectSql() != null) {
            ret.setSelectSql(createTablePreparedData.getSelectSql());
        }
        logicalCreateTable.setAffectedRows(ret.getAffectRows());
        return ret.create();
    }

    protected DdlJob buildCreatePartitionTableJob(LogicalCreateTable logicalCreateTable,
                                                  ExecutionContext executionContext) {
        PartitionTableType partitionTableType = PartitionTableType.SINGLE_TABLE;
        if (logicalCreateTable.isPartitionTable()) {
            partitionTableType = PartitionTableType.PARTITION_TABLE;
        } else if (logicalCreateTable.isBroadCastTable()) {
            partitionTableType = PartitionTableType.BROADCAST_TABLE;
        }
        CreateTablePreparedData createTablePreparedData = logicalCreateTable.getCreateTablePreparedData();

        // 构建物理表拓扑
        DdlPhyPlanBuilder createTableBuilder =
            new CreatePartitionTableBuilder(logicalCreateTable.relDdl, createTablePreparedData, executionContext,
                partitionTableType).build();
        PhysicalPlanData physicalPlanData = createTableBuilder.genPhysicalPlanData();
        Engine tableEngine = ((SqlCreateTable) logicalCreateTable.relDdl.sqlNode).getEngine();
        ArchiveMode archiveMode = ((SqlCreateTable) logicalCreateTable.relDdl.sqlNode).getArchiveMode();

        if (Engine.isFileStore(tableEngine)) {
            CreatePartitionOssTableJobFactory ret = new CreatePartitionOssTableJobFactory(
                createTablePreparedData.isAutoPartition(), createTablePreparedData.isTimestampColumnDefault(),
                createTablePreparedData.getSpecialDefaultValues(),
                createTablePreparedData.getSpecialDefaultValueFlags(),
                physicalPlanData, executionContext, createTablePreparedData, tableEngine, archiveMode);
            if (createTablePreparedData.getSelectSql() != null) {
                ret.setSelectSql(createTablePreparedData.getSelectSql());
            }
            logicalCreateTable.setAffectedRows(ret.getAffectRows());
            return ret.create();
        }

        PartitionInfo partitionInfo = createTableBuilder.getPartitionInfo();
        CreatePartitionTableJobFactory ret = new CreatePartitionTableJobFactory(
            createTablePreparedData.isAutoPartition(), createTablePreparedData.isTimestampColumnDefault(),
            createTablePreparedData.getSpecialDefaultValues(),
            createTablePreparedData.getSpecialDefaultValueFlags(),
            createTablePreparedData.getAddedForeignKeys(),
            physicalPlanData, executionContext, createTablePreparedData, partitionInfo);

        if (createTablePreparedData.getSelectSql() != null) {
            ret.setSelectSql(createTablePreparedData.getSelectSql());
        }
        logicalCreateTable.setAffectedRows(ret.getAffectRows());
        return ret.create();
    }

    private DdlJob buildCreateTableWithGsiJob(LogicalCreateTable logicalCreateTable,
                                              ExecutionContext executionContext) {
        CreateTableWithGsiPreparedData createTableWithGsiPreparedData =
            logicalCreateTable.getCreateTableWithGsiPreparedData();
        CreateTableWithGsiJobFactory ret = new CreateTableWithGsiJobFactory(
            logicalCreateTable.relDdl,
            createTableWithGsiPreparedData,
            executionContext
        );
        if (createTableWithGsiPreparedData.getPrimaryTablePreparedData().getSelectSql() != null) {
            ret.setSelectSql(createTableWithGsiPreparedData.getPrimaryTablePreparedData().getSelectSql());
        }
        logicalCreateTable.setAffectedRows(ret.getAffectRows());
        return ret.create();
    }

    private DdlJob buildCreatePartitionTableWithGsiJob(LogicalCreateTable logicalCreateTable,
                                                       ExecutionContext executionContext) {
        CreateTableWithGsiPreparedData createTableWithGsiPreparedData =
            logicalCreateTable.getCreateTableWithGsiPreparedData();
        CreatePartitionTableWithGsiJobFactory ret = new CreatePartitionTableWithGsiJobFactory(
            logicalCreateTable.relDdl,
            createTableWithGsiPreparedData,
            executionContext
        );
        if (createTableWithGsiPreparedData.getPrimaryTablePreparedData().getSelectSql() != null) {
            ret.setSelectSql(createTableWithGsiPreparedData.getPrimaryTablePreparedData().getSelectSql());
        }
        logicalCreateTable.setAffectedRows(ret.getAffectRows());
        return ret.create();
    }

    private final static String CREATE_TABLE = "CREATE TABLE";
    private final static String CREATE_TABLE_IF_NOT_EXISTS = "CREATE TABLE IF NOT EXISTS";

    public static String generateCreateTableSqlForLike(SqlCreateTable sqlCreateTable,
                                                       ExecutionContext executionContext) {
        SqlIdentifier sourceTableName = (SqlIdentifier) sqlCreateTable.getLikeTableName();
        String sourceTableSchema =
            sourceTableName.names.size() > 1 ? sourceTableName.names.get(0) : executionContext.getSchemaName();

        IRepository sourceTableRepository = ExecutorContext
            .getContext(sourceTableSchema)
            .getTopologyHandler()
            .getRepositoryHolder()
            .get(Group.GroupType.MYSQL_JDBC.toString());
        LogicalShowCreateTableHandler logicalShowCreateTablesHandler =
            new LogicalShowCreateTableHandler(sourceTableRepository);

        SqlShowCreateTable sqlShowCreateTable =
            SqlShowCreateTable.create(SqlParserPos.ZERO, sqlCreateTable.getLikeTableName(), true);
        ExecutionContext copiedContext = executionContext.copy();
        copiedContext.setSchemaName(sourceTableSchema);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(copiedContext);
        ExecutionPlan showCreateTablePlan = Planner.getInstance().getPlan(sqlShowCreateTable, plannerContext);
        LogicalShow logicalShowCreateTable = (LogicalShow) showCreateTablePlan.getPlan();

        Cursor showCreateTableCursor =
            logicalShowCreateTablesHandler.handle(logicalShowCreateTable, executionContext);

        String createTableSql = null;

        Row showCreateResult = showCreateTableCursor.next();
        if (showCreateResult != null && showCreateResult.getString(1) != null) {
            createTableSql = showCreateResult.getString(1);
        } else {
            GeneralUtil.nestedException("Get reference table architecture failed.");
        }

        if (sqlCreateTable.isIfNotExists()) {
            createTableSql = createTableSql.replace(CREATE_TABLE, CREATE_TABLE_IF_NOT_EXISTS);
        }

        return createTableSql;
    }

    public LogicalCreateTable generatePlan(LogicalCreateTable logicalCreateTable, ExecutionContext executionContext) {
        // 获取select from 中的表结构
        SqlCreateTable sqlCreateTable = (SqlCreateTable) logicalCreateTable.relDdl.sqlNode;
        sqlCreateTable.setDBPartition(DbInfoManager.getInstance().isNewPartitionDb(logicalCreateTable.getSchemaName()));
        SqlSelect sqlSelect = (SqlSelect) sqlCreateTable.getQuery();
        String createLike = null;
        if (sqlCreateTable.getAsTableName() != null) {
            // as table name 改写成 select * from
            SqlPrettyWriter writer = new SqlPrettyWriter(MysqlSqlDialect.DEFAULT);
            writer.setAlwaysUseParentheses(true);
            writer.setSelectListItemsOnSeparateLines(false);
            writer.setIndentation(0);
            writer.keyword("SELECT");
            writer.keyword("*");
            writer.keyword("FROM");
            writer.keyword(((SqlIdentifier) sqlCreateTable.getAsTableName()).getLastName());
            String asTable = writer.toSqlString().getSql();
            ExecutionPlan selectPlan = Planner.getInstance().plan(asTable, executionContext);
            sqlSelect = (SqlSelect) selectPlan.getAst();

            SqlPrettyWriter writer1 = new SqlPrettyWriter(MysqlSqlDialect.DEFAULT);
            writer1.setAlwaysUseParentheses(true);
            writer1.setSelectListItemsOnSeparateLines(false);
            writer1.setIndentation(0);
            writer1.keyword("CREATE");
            writer1.keyword("TABLE");
            sqlCreateTable.getName().unparse(writer1, 0, 0);
            writer1.keyword("LIKE");
            writer1.keyword(((SqlIdentifier) sqlCreateTable.getAsTableName()).getLastName());
            createLike = writer1.toSqlString().getSql();
        }
        ExecutionContext copiedContext = executionContext.copy();

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(copiedContext);
        ExecutionPlan selectPlan = Planner.getInstance().getPlan(sqlSelect, plannerContext);
        RelNode selectRelNode = selectPlan.getPlan();
        SqlSelect selectFinshed = (SqlSelect) selectPlan.getAst();
        SqlNodeList selectList = selectFinshed.getSelectList();
        // 获取数据行的类型
        List<RelDataTypeField> selectRowTypes = selectRelNode.getRowType().getFieldList();

        List<Pair<SqlIdentifier, SqlColumnDeclaration>> createCols = sqlCreateTable.getColDefs();

        List<Pair<SqlIdentifier, SqlColumnDeclaration>> overlapCols = new ArrayList<>();
        List<Pair<SqlIdentifier, SqlColumnDeclaration>> newDelCols = new ArrayList<>();
        for (RelDataTypeField selectCol : selectRowTypes) {
            // 如果create没语句中没有找到，说明是新添加的列
            int colIndex = getIndex(createCols, selectCol.getName());
            if (colIndex == -1) {
                // 新创建的列

                SqlIdentifier delId = new SqlIdentifier(selectCol.getName(), SqlParserPos.ZERO);
                BasicSqlType type = (BasicSqlType) selectCol.getType();
                // 如果投影下来为 null

                SqlDataTypeSpec castSpec = new SqlDataTypeSpec(
                    selectCol.getValue().getSqlIdentifier(),
                    selectCol.getValue().getPrecision(),
                    selectCol.getValue().getScale(),
                    selectCol.getValue().getCharset() == null ? null : type.getCharset().name(),
                    null,
                    SqlParserPos.ZERO);

//                if (type.getSqlTypeName().getName().equalsIgnoreCase("null")) {
//                    castSpec = new SqlDataTypeSpec(
//                        new SqlIdentifier()
//                        0,
//                        0,
//                        selectCol.getValue().getCharset() == null ? null : type.getCharset().name(),
//                        null,
//                        SqlParserPos.ZERO);
//                }
                boolean isNull = selectCol.getValue().isNullable();
                SqlColumnDeclaration del = new SqlColumnDeclaration(SqlParserPos.ZERO, delId, castSpec,
                    isNull ? SqlColumnDeclaration.ColumnNull.NULL : SqlColumnDeclaration.ColumnNull.NOTNULL,
                    isNull ? SqlLiteral.createNull(SqlParserPos.ZERO) : null,
                    null, false, null,
                    SqlLiteral.createCharString("select key", SqlParserPos.ZERO), null, null, null, false,
                    null, 0, -1, 0, false, false, null);

                newDelCols.add(new Pair<SqlIdentifier, SqlColumnDeclaration>(delId, del));
            } else {
                // overlap的列，需要放到中间的位置
                overlapCols.add(createCols.get(colIndex));
                createCols.remove(colIndex);
            }
        }
        // 先是create语句声明的列，然后是overlap的列，最后是new的列
        for (Pair<SqlIdentifier, SqlColumnDeclaration> addCol : overlapCols) {
            createCols.add(addCol);
        }
        for (Pair<SqlIdentifier, SqlColumnDeclaration> addCol : newDelCols) {
            createCols.add(addCol);
        }
        int defaultIndex = getIndex(createCols, new String("_drds_implicit_id_"));
        if (defaultIndex != -1) {
            createCols.remove(defaultIndex);
        }
        // 修改coldefs，然后把子查询置为null

        // 需要修改原始的 sql 语句
        // String originSql = sqlCreateTable.getOriginalSql();
        SqlCreateTable sqlCreateTableTmp = sqlCreateTable.clone(SqlParserPos.ZERO);

        sqlCreateTableTmp.setSelect(true);
        sqlCreateTableTmp.setColDefs(createCols);
        sqlCreateTableTmp.setQuery(null);
        sqlCreateTableTmp.setSourceSql(null);
        // TODO：除了primary_key可能还有其他的implict key 需要删除
        if (sqlCreateTableTmp.getPrimaryKey().getIndexName() != null && sqlCreateTableTmp.getPrimaryKey().getIndexName()
            .getLastName().equals("_drds_implicit_pk_")) {
            sqlCreateTableTmp.setPrimaryKey(null);
        }

        SqlPrettyWriter writer = new SqlPrettyWriter(MysqlSqlDialect.DEFAULT);
        writer.setAlwaysUseParentheses(true);
        writer.setSelectListItemsOnSeparateLines(false);
        writer.setIndentation(0);
        sqlCreateTableTmp.unparse(writer, 0, 0);
        String createSelectSql = writer.toSqlString().getSql();

        if (sqlCreateTable.getAsTableName() != null) {
            createSelectSql = createLike;
        }

        ExecutionPlan createTableLikeSqlPlan = Planner.getInstance().plan(createSelectSql, executionContext);
        LogicalCreateTable logicalCreateTableLikeRelNode = (LogicalCreateTable) createTableLikeSqlPlan.getPlan();
        logicalCreateTable = logicalCreateTableLikeRelNode;
        logicalCreateTable.getSqlCreate().setSelect(true);
        logicalCreateTable.setSqlSelect((SqlSelect) selectFinshed);
        logicalCreateTable.setIgnore(sqlCreateTable.isIgnore());
        logicalCreateTable.setReplace(sqlCreateTable.isReplace1());
        return logicalCreateTable;
    }

    @Override
    protected Cursor buildResultCursor(BaseDdlOperation baseDdl, DdlJob ddlJob, ExecutionContext ec) {
        int affectRows = 0;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            DdlEngineTaskAccessor accessor = new DdlEngineTaskAccessor();
            accessor.setConnection(metaDbConn);

            Long jobId = ec.getDdlJobId();
            Long taskId = 0L;
//            if (ddlJob instanceof ExecutableDdlJob4CreateTable) {
//                taskId = ((ExecutableDdlJob4CreateTable) ddlJob).getInsertIntoTask().getTaskId();
//            } else if (ddlJob instanceof ExecutableDdlJob4CreatePartitionTable) {
//                taskId = ((ExecutableDdlJob4CreatePartitionTable) ddlJob).getInsertIntoTask().getTaskId();
//            }
//            else if (ddlJob instanceof ExecutableDdlJob4CreatePartitionGsi) {
//                // taskId = ((ExecutableDdlJob4CreatePartitionGsi) ddlJob).getInsertIntoTask().getTaskId();
//            } else if (ddlJob instanceof ExecutableDdlJob4CreateGsi) {
//                // taskId = ((ExecutableDdlJob4CreateGsi) ddlJob).getInsertIntoTask().getTaskId();
//            }

            DdlEngineTaskRecord record = accessor.query(jobId, taskId);
            if (record == null) {
                record = accessor.archiveQuery(jobId, taskId);
            }
            if (record != null) {
                InsertIntoTask task = (InsertIntoTask) deSerializeTask(record.name, record.value);
                affectRows = task.getAffectRows();
            }
        } catch (Throwable ex) {
            //在metadb获取affectRows失败，但是实际上DDL任务执行成功了
            LoggerFactory.getLogger(LogicalInsertOverwriteHandler.class)
                .warn("Insert Overwrite get AffectRows failed", ex);
        }

        return new AffectRowCursor(affectRows);
    }

}
