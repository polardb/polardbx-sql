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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
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
import com.alibaba.polardbx.executor.ddl.job.factory.PureCdcDdlMark4CreateTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.ReimportTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.oss.CreatePartitionOssTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.InsertIntoTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.CheckAndPrepareColumnarIndexPartDefTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.PreparingFormattedCurrDatetimeTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobContext;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlTaskSqlBuilder;
import com.alibaba.polardbx.executor.ddl.job.validator.ColumnValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ConstraintValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ForeignKeyValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.handler.LogicalShowCreateTableHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.executor.utils.StandardToEnterpriseEditionUtil;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.DefaultExprUtil;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.LikeTableInfo;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.ttl.TtlArchiveKind;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlMetaValidationUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.CreateTable;
import org.apache.calcite.rel.type.RelDataTypeField;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupUtils;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.SqlPartitionByHash;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.sql.SqlSubPartitionBy;
import org.apache.calcite.sql.SqlSubPartitionByHash;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
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

        // return job factory for cdc ddl mark in case of creating table if not exits, if table already exist
        final String schemaName = logicalDdlPlan.getSchemaName();
        final String logicalTableName = logicalDdlPlan.getTableName();
        boolean tableExists = TableValidator.checkIfTableExists(schemaName, logicalTableName);
        if (tableExists && sqlCreateTable.isIfNotExists()) {
            LimitValidator.validateTableNameLength(schemaName);
            LimitValidator.validateTableNameLength(logicalTableName);

            // Prompt "show warning" only.
            DdlHelper.storeFailedMessage(schemaName, ERROR_TABLE_EXISTS,
                " Table '" + logicalTableName + "' already exists", executionContext);
            executionContext.getDdlContext().setUsingWarning(true);

            return new PureCdcDdlMark4CreateTableJobFactory(schemaName, logicalTableName).create();
        }

        //import table, reimport table
        boolean importTable = executionContext.getParamManager().getBoolean(ConnectionParams.IMPORT_TABLE);
        boolean reImportTable = executionContext.getParamManager().getBoolean(ConnectionParams.REIMPORT_TABLE);
        if (importTable) {
            logicalCreateTable.setImportTable(true);
        }
        if (reImportTable) {
            logicalCreateTable.setReImportTable(true);
        }

        LikeTableInfo likeTableInfo = null;
        if (sqlCreateTable.getLikeTableName() != null) {

            SqlCreateTable createTableLikeSqlAst = null;
            PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);

            Boolean[] needConvertToCreateArcCciArr = new Boolean[1];
            Boolean[] needIgnoreArchiveCciForCreateTableLikeArr = new Boolean[1];
            needConvertToCreateArcCciArr[0] = false;
            needIgnoreArchiveCciForCreateTableLikeArr[0] = false;
            checkIfAllowedCreatingTableLikeTtlDefinedTable(sqlCreateTable, executionContext,
                needConvertToCreateArcCciArr,
                needIgnoreArchiveCciForCreateTableLikeArr);
            boolean needConvertToCreateArcCci = needConvertToCreateArcCciArr[0];
            boolean needIgnoreArchiveCciForCreateTableLike = needIgnoreArchiveCciForCreateTableLikeArr[0];
            if (needConvertToCreateArcCci) {
                return buildCreateColumnarIndexJobForArchiveTable(logicalCreateTable, executionContext,
                    likeTableInfo);
            }

            String sourceCreateTableSql = generateCreateTableSqlForLike(sqlCreateTable, executionContext);
            MySqlCreateTableStatement stmt =
                (MySqlCreateTableStatement) FastsqlUtils.parseSql(sourceCreateTableSql).get(0);

            // remove arc cci of source ttl-table for new created tbl
            if (needIgnoreArchiveCciForCreateTableLike) {
                stmt.removeArchiveCciInfoForTtlDefinitionOptionIfNeed();
            }
            // remove cci if not need in create table like
            if (executionContext.getParamManager().getBoolean(ConnectionParams.IGNORE_CCI_WHEN_CREATE_SHADOW_TABLE)) {
                stmt.removeCciIfNeed();
            }
            // change table name to new created tbl
            stmt.getTableSource()
                .setSimpleName(SqlIdentifier.surroundWithBacktick(logicalCreateTable.getTableName()));
            // set table engine
            String shadowTableEngine =
                executionContext.getParamManager().getString(ConnectionParams.CREATE_SHADOW_TABLE_ENGINE);
            if (!shadowTableEngine.isEmpty()) {
                stmt.setEngine(Engine.of(shadowTableEngine).name());
            }

            // create table ast of target
            final String targetCreateTableSql = stmt.toString();
            final SqlCreateTable targetTableAst = (SqlCreateTable)
                new FastsqlParser().parse(targetCreateTableSql, executionContext).get(0);
            createTableLikeSqlAst = targetTableAst;
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
            // set table engine
            if (!shadowTableEngine.isEmpty()) {
                createTableLikeSqlAst.setEngine(Engine.of(shadowTableEngine));
            }

            // set archive mode if there is engine option in user sql
            ArchiveMode archiveMode;
            if ((archiveMode = sqlCreateTable.getArchiveMode()) != null) {
                createTableLikeSqlAst.setArchiveMode(archiveMode);
            }

            if (archiveMode != null) {
                if (!Engine.isFileStore(engine)) {
                    throw GeneralUtil.nestedException(
                        "cannot create table using ARCHIVE_MODE if the engine of target table is INNODB.");
                }
            }

            List<String> dictColumns;
            if ((dictColumns = sqlCreateTable.getDictColumns()) != null) {
                createTableLikeSqlAst.setDictColumns(dictColumns);
            }

            if (!Engine.isFileStore(engine) && dictColumns != null) {
                throw GeneralUtil.nestedException(
                    "cannot create table with DICTIONARY_COLUMNS if the engine of target table is INNODB.");
            }

            SqlIdentifier sourceTableName = (SqlIdentifier) sqlCreateTable.getLikeTableName();
            String sourceSchema =
                sourceTableName.names.size() > 1 ? sourceTableName.names.get(0) : executionContext.getSchemaName();
            likeTableInfo = new LikeTableInfo(sourceSchema, sourceTableName.getLastName());

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
                        createTableLikeSqlAst.setLoadTableSchema(executionContext.getSchemaName());
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
        final Long versionId = DdlUtils.generateVersionId(executionContext);
        logicalCreateTable.setDdlVersionId(versionId);
        if (flag) {
            String createTableSql = logicalCreateTable.getNativeSql();
            String insertIntoSql = logicalCreateTable.getCreateTablePreparedData().getSelectSql();
            return new CreateTableSelectJobFactory(logicalCreateTable.getSchemaName(),
                logicalCreateTable.getTableName(), executionContext, createTableSql, insertIntoSql).create();
        }
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(logicalCreateTable.getSchemaName());
        if (!isNewPartDb) {
            if (logicalCreateTable.isWithGsi()) {
                return buildCreateTableWithGsiJob(logicalCreateTable, executionContext, likeTableInfo);
            } else {
                return buildCreateTableJob(logicalCreateTable, executionContext, likeTableInfo);
            }
        } else {
            if (logicalCreateTable.isWithGsi()) {
                return buildCreatePartitionTableWithGsiJob(logicalCreateTable, executionContext, likeTableInfo);
            } else {
                return buildCreatePartitionTableJob(logicalCreateTable, executionContext, likeTableInfo);
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
        if (StringUtils.isEmpty(tableGroupName) || sqlCreateTable.isWithImplicitTableGroup()) {
            return;
        }

        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        if (tableGroupConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS, tableGroupName);
        }
        String partitionDef = TableGroupUtils.getPreDefinePartitionInfo(tableGroupConfig, ec);
        if (StringUtils.isEmpty(partitionDef)) {
            return;
        }
        boolean isSingle = tableGroupConfig.getTableGroupRecord().isSingleTableGroup()
            || tableGroupConfig.isEmpty() && partitionDef.trim().equalsIgnoreCase("SINGLE");
        if (!isSingle) {

            SqlPartitionBy tableSqlPartitionBy = (SqlPartitionBy) sqlCreateTable.getSqlPartition();
            SQLPartitionBy fastSqlPartitionBy = FastsqlUtils.parsePartitionBy(partitionDef, true);
            if (!tableGroupConfig.isEmpty()) {
                //i.e.  partition by udf_hash(Mymurmurhash64var(c1)), Mymurmurhash64var(int) is not a valid column definition,
                // but it will be as part of result when /*+TDDL:cmd_extra(SHOW_HASH_PARTITIONS_BY_RANGE=TRUE)*/ show create full table
                fastSqlPartitionBy.getColumnsDefinition().clear();
            }
            if (fastSqlPartitionBy.getSubPartitionBy() != null) {
                fastSqlPartitionBy.getSubPartitionBy().getColumnsDefinition().clear();
            }
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

            normlizePartitionBy(tableGroupSqlPartitionBy);
            normalizeSubPartitionBy(sqlSubPartitionBy);

            sqlCreateTable.setSqlPartition(tableGroupSqlPartitionBy);

            SqlConverter sqlConverter = SqlConverter.getInstance(schemaName, ec);
            PlannerContext plannerContext = PlannerContext.getPlannerContext(createTable.getCluster());

            Map<SqlNode, RexNode> partRexInfoCtx =
                sqlConverter.convertPartition(tableGroupSqlPartitionBy, plannerContext);

            ((CreateTable) (createTable)).getPartBoundExprInfo().putAll(partRexInfoCtx);
        } else {
            sqlCreateTable.setSingle(true);
            sqlCreateTable.setSqlPartition(null);
        }
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

        //validate import/reimport table
        boolean isImportTable = executionContext.getParamManager().getBoolean(ConnectionParams.IMPORT_TABLE);
        boolean reImportTable = executionContext.getParamManager().getBoolean(ConnectionParams.REIMPORT_TABLE);
        if (isImportTable || reImportTable) {
            String locality = sqlCreateTable.getLocality();
            if (StringUtil.isNullOrEmpty(locality)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "physical table's locality is required when import table");
            }
            LocalityDesc localityDesc = LocalityDesc.parse(locality);
            if (localityDesc.getDnList().size() != 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "only one DN Id is allowed");
            }
            String dnName = localityDesc.getDnList().get(0);
            List<DbGroupInfoRecord> dbGroupInfoRecords =
                DbTopologyManager.getAllDbGroupInfoRecordByInstId(schemaName, dnName);
            if (dbGroupInfoRecords.size() != 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "it's not allowed to import table when db group size exceeds 1");
            }

            String phyDbName = dbGroupInfoRecords.get(0).phyDbName;

            Set<String> phyTables =
                StandardToEnterpriseEditionUtil.queryPhysicalTableListFromPhysicalDabatase(dnName, phyDbName);
            if (!phyTables.contains(logicalTableName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format("physical table [%s] not found when %s table in schema [%s] instantId [%s]",
                        logicalTableName,
                        isImportTable ? "import" : "reimport",
                        schemaName, dnName)
                );
            }

            if (reImportTable) {
                Set<String> logicalTables =
                    StandardToEnterpriseEditionUtil.getTableNamesFromLogicalDatabase(schemaName, executionContext);
                if (!logicalTables.contains(logicalTableName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        String.format("logical table [%s] not found when reimport table in schema [%s]",
                            logicalTableName, schemaName));
                }
            }

            //use database-level locality, so no need of table-level locality
            sqlCreateTable.setLocality(null);
        }

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
            // do nothing
        } else if (reImportTable) {
            // do nothing
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

    protected DdlJob buildCreateTableJob(LogicalCreateTable logicalCreateTable, ExecutionContext executionContext,
                                         LikeTableInfo likeTableInfo) {
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
            createTablePreparedData.getDdlVersionId(),
            executionContext,
            likeTableInfo
        );
        logicalCreateTable.setAffectedRows(ret.getAffectRows());
        if (createTablePreparedData.getSelectSql() != null) {
            ret.setSelectSql(createTablePreparedData.getSelectSql());
        }
        logicalCreateTable.setAffectedRows(ret.getAffectRows());
        return ret.create();
    }

    protected DdlJob buildCreatePartitionTableJob(LogicalCreateTable logicalCreateTable,
                                                  ExecutionContext executionContext,
                                                  LikeTableInfo likeTableInfo) {
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

        buildTtlInfoIfNeed(logicalCreateTable, executionContext, createTableBuilder.getPartitionInfo(),
            createTablePreparedData);

        PhysicalPlanData physicalPlanData = createTableBuilder.genPhysicalPlanData();
        Engine tableEngine = ((SqlCreateTable) logicalCreateTable.relDdl.sqlNode).getEngine();
        ArchiveMode archiveMode = ((SqlCreateTable) logicalCreateTable.relDdl.sqlNode).getArchiveMode();
        List<String> dictColumns = ((SqlCreateTable) logicalCreateTable.relDdl.sqlNode).getDictColumns();
        CreateTableJobFactory ret = null;
        if (Engine.isFileStore(tableEngine)) {
            ret = new CreatePartitionOssTableJobFactory(
                createTablePreparedData.isAutoPartition(), createTablePreparedData.isTimestampColumnDefault(),
                createTablePreparedData.getSpecialDefaultValues(),
                createTablePreparedData.getSpecialDefaultValueFlags(),
                physicalPlanData, executionContext, createTablePreparedData, tableEngine, archiveMode, dictColumns);
            if (createTablePreparedData.getSelectSql() != null) {
                ret.setSelectSql(createTablePreparedData.getSelectSql());
            }
            logicalCreateTable.setAffectedRows(ret.getAffectRows());
            return ret.create();
        }

        PartitionInfo partitionInfo = createTableBuilder.getPartitionInfo();
        if (logicalCreateTable.isReImportTable()) {
            return new ReimportTableJobFactory(createTablePreparedData.isAutoPartition(),
                createTablePreparedData.isTimestampColumnDefault(),
                createTablePreparedData.getSpecialDefaultValues(),
                createTablePreparedData.getSpecialDefaultValueFlags(),
                createTablePreparedData.getAddedForeignKeys(),
                physicalPlanData, executionContext, createTablePreparedData, partitionInfo).create();

        } else {
            ret = new CreatePartitionTableJobFactory(
                createTablePreparedData.isAutoPartition(), createTablePreparedData.isTimestampColumnDefault(),
                createTablePreparedData.getSpecialDefaultValues(),
                createTablePreparedData.getSpecialDefaultValueFlags(),
                createTablePreparedData.getAddedForeignKeys(),
                physicalPlanData, executionContext, createTablePreparedData, partitionInfo, likeTableInfo);

            if (createTablePreparedData.getSelectSql() != null) {
                ret.setSelectSql(createTablePreparedData.getSelectSql());
            }
            logicalCreateTable.setAffectedRows(ret.getAffectRows());
            return ret.create();
        }
    }

    protected static void buildTtlInfoIfNeed(LogicalCreateTable logicalCreateTable,
                                             ExecutionContext executionContext,
                                             PartitionInfo logicalTablePartInfo,
                                             CreateTablePreparedData primTablePreparedData) {
        PartitionInfo partInfo = logicalTablePartInfo;
        TableMeta tableMeta = primTablePreparedData.getTableMeta();
        TtlDefinitionInfo ttlDefinitionInfo =
            TtlUtil.createTtlDefinitionInfoBySqlCreateTable((SqlCreateTable) logicalCreateTable.relDdl.sqlNode,
                tableMeta, partInfo, executionContext);
        primTablePreparedData.setTtlDefinitionInfo(ttlDefinitionInfo);
    }

    private DdlJob buildCreateTableWithGsiJob(LogicalCreateTable logicalCreateTable,
                                              ExecutionContext executionContext,
                                              LikeTableInfo likeTableInfo) {
        CreateTableWithGsiPreparedData createTableWithGsiPreparedData =
            logicalCreateTable.getCreateTableWithGsiPreparedData();
        createTableWithGsiPreparedData.getPrimaryTablePreparedData().setLikeTableInfo(likeTableInfo);

        executionContext.setForbidBuildLocalIndexLater(true);
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
                                                       ExecutionContext executionContext,
                                                       LikeTableInfo likeTableInfo) {
        CreateTableWithGsiPreparedData createTableWithGsiPreparedData =
            logicalCreateTable.getCreateTableWithGsiPreparedData();
        createTableWithGsiPreparedData.getPrimaryTablePreparedData().setLikeTableInfo(likeTableInfo);

        executionContext.setForbidBuildLocalIndexLater(true);
        CreatePartitionTableWithGsiJobFactory ret = new CreatePartitionTableWithGsiJobFactory(
            logicalCreateTable.relDdl,
            createTableWithGsiPreparedData,
            executionContext
        );

        if (createTableWithGsiPreparedData.getPrimaryTablePreparedData().getSelectSql() != null) {
            ret.setSelectSql(createTableWithGsiPreparedData.getPrimaryTablePreparedData().getSelectSql());
        }
        logicalCreateTable.setAffectedRows(ret.getAffectRows());

        PartitionInfo primTblPartInfo =
            ret.getCreatePartitionTableWithGsiBuilder().getPrimaryTableBuilder().getPartitionInfo();
        CreateTablePreparedData primPrepData = createTableWithGsiPreparedData.getPrimaryTablePreparedData();
        buildTtlInfoIfNeed(logicalCreateTable, executionContext, primTblPartInfo, primPrepData);

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
        List<ColumnMeta> columnMetaList = selectPlan.getCursorMeta().getColumns();
        List<RelDataTypeField> selectRowTypes = new ArrayList<>(columnMetaList.size());
        for (int i = 0; i < columnMetaList.size(); i++) {
            ColumnMeta columnMeta = columnMetaList.get(i);
            selectRowTypes.add(
                new RelDataTypeFieldImpl(columnMeta.getOriginColumnName(), i, columnMeta.getField().getRelType()));
        }

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

    protected void normlizePartitionBy(SqlPartitionBy partitionBy) {
        boolean key = partitionBy instanceof SqlPartitionByHash &&
            ((SqlPartitionByHash) partitionBy).isKey();
        if (key) {
            int partCols = partitionBy.getColumns().size();
            for (SqlNode sqlPartition : partitionBy.getPartitions()) {
                SqlPartitionValue sqlPartitionValue = ((SqlPartition) sqlPartition).getValues();
                while (sqlPartitionValue.getItems().size() < partCols) {
                    SqlPartitionValueItem item =
                        new SqlPartitionValueItem(new SqlIdentifier("MAXVALUE", SqlParserPos.ZERO));
                    item.setMaxValue(true);
                    sqlPartitionValue.getItems().add(item);
                }
            }
        }
        SqlSubPartitionBy subPartitionBy = partitionBy.getSubPartitionBy();
        if (subPartitionBy == null) {
            return;
        }
        boolean keySubPart = subPartitionBy instanceof SqlSubPartitionByHash &&
            ((SqlSubPartitionByHash) subPartitionBy).isKey();
        if (!keySubPart) {
            return;
        }
        int subPartCols = subPartitionBy.getColumns().size();
        for (SqlNode sqlPartition : partitionBy.getPartitions()) {
            List<SqlNode> subPartitions = ((SqlPartition) sqlPartition).getSubPartitions();
            for (SqlNode sqlSubPartition : subPartitions) {
                SqlPartitionValue sqlPartitionValue = ((SqlSubPartition) sqlSubPartition).getValues();
                while (sqlPartitionValue.getItems().size() < subPartCols) {
                    SqlPartitionValueItem item =
                        new SqlPartitionValueItem(new SqlIdentifier("MAXVALUE", SqlParserPos.ZERO));
                    item.setMaxValue(true);
                    sqlPartitionValue.getItems().add(item);
                }
            }

        }
    }

    protected void normalizeSubPartitionBy(SqlSubPartitionBy subPartitionBy) {
        if (subPartitionBy == null) {
            return;
        }
        boolean key = subPartitionBy instanceof SqlSubPartitionByHash &&
            ((SqlSubPartitionByHash) subPartitionBy).isKey();
        if (!key) {
            return;
        }
        int partCols = subPartitionBy.getColumns().size();
        for (SqlNode sqlPartition : subPartitionBy.getSubPartitions()) {
            SqlPartitionValue sqlPartitionValue = ((SqlSubPartition) sqlPartition).getValues();
            while (sqlPartitionValue.getItems().size() < partCols) {
                SqlPartitionValueItem item =
                    new SqlPartitionValueItem(new SqlIdentifier("MAXVALUE", SqlParserPos.ZERO));
                item.setMaxValue(true);
                sqlPartitionValue.getItems().add(item);
            }
        }
    }

    protected DdlJob buildCreateColumnarIndexJobForArchiveTable(LogicalCreateTable logicalCreateTable,
                                                                ExecutionContext ec,
                                                                LikeTableInfo likeTableInfo) {

        SqlCreateTable sqlCreateTable = (SqlCreateTable) logicalCreateTable.relDdl.sqlNode;
        SqlIdentifier sourceTableNameAst = (SqlIdentifier) sqlCreateTable.getLikeTableName();
        String sourceTableSchema =
            SQLUtils.normalizeNoTrim(
                sourceTableNameAst.names.size() > 1 ? sourceTableNameAst.names.get(0) : ec.getSchemaName());
        String sourceTableName = SQLUtils.normalizeNoTrim(sourceTableNameAst.getLastName());

        String targetTableSchema = logicalCreateTable.getSchemaName();
        String targetTableName = logicalCreateTable.getTableName();

        TtlDefinitionInfo ttlInfo = TtlUtil.getTtlDefInfoBySchemaAndTable(sourceTableSchema, sourceTableName, ec);
        TtlMetaValidationUtil.validateAllowedBoundingArchiveTable(ttlInfo, ec, true, targetTableSchema,
            targetTableName);

        String createViewSqlForArcTbl =
            TtlTaskSqlBuilder.buildCreateViewSqlForArcTbl(targetTableSchema, targetTableName, ttlInfo);
        String dropViewSqlForArcTbl =
            TtlTaskSqlBuilder.buildDropViewSqlFroArcTbl(targetTableSchema, targetTableName, ttlInfo);

        List<DdlTask> taskList = new ArrayList<>();

        SubJobTask createViewSubJobTask =
            new SubJobTask(targetTableSchema, createViewSqlForArcTbl, dropViewSqlForArcTbl);
        createViewSubJobTask.setParentAcquireResource(true);

        String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
        TtlArchiveKind currArcKind = TtlArchiveKind.of(ttlInfo.getTtlInfoRecord().getArcKind());
        TtlArchiveKind newArcKindToBeSet = currArcKind;
        if (currArcKind == TtlArchiveKind.UNDEFINED) {
            newArcKindToBeSet = TtlArchiveKind.ROW;
        }
        String modifyTtlBindArcTblSql =
            TtlTaskSqlBuilder.buildModifyTtlSqlForBindArcTbl(ttlTblSchema, ttlTblName, targetTableSchema,
                targetTableName, newArcKindToBeSet);
        String modifyTtlBindArcTblSqlForRollback =
            TtlTaskSqlBuilder.buildModifyTtlSqlForBindArcTbl(ttlTblSchema, ttlTblName, "", "",
                TtlArchiveKind.UNDEFINED);
        SubJobTask modifyTtlForBindArcTblSubTask =
            new SubJobTask(sourceTableSchema, modifyTtlBindArcTblSql, modifyTtlBindArcTblSqlForRollback);
        modifyTtlForBindArcTblSubTask.setParentAcquireResource(true);

        TtlJobContext jobContext = TtlJobContext.buildFromTtlInfo(ttlInfo);
        PreparingFormattedCurrDatetimeTask preparingFormattedCurrDatetimeTask =
            new PreparingFormattedCurrDatetimeTask(sourceTableSchema, sourceTableName);
        preparingFormattedCurrDatetimeTask.setJobContext(jobContext);
        CheckAndPrepareColumnarIndexPartDefTask prepareCreateCiSqlTask =
            new CheckAndPrepareColumnarIndexPartDefTask(sourceTableSchema, sourceTableName, targetTableSchema,
                targetTableName);
        String createCiSqlForArcTblSubJobName =
            TtlTaskSqlBuilder.buildSubJobTaskNameForCreateColumnarIndexBySpecifySubJobStmt();
        String dropCiSqlForArcTbl =
            TtlTaskSqlBuilder.buildDropColumnarIndexSqlForArcTbl(ttlInfo, targetTableSchema, targetTableName);
        SubJobTask createCiSubJobTask =
            new SubJobTask(sourceTableSchema, createCiSqlForArcTblSubJobName, dropCiSqlForArcTbl);
        createCiSubJobTask.setParentAcquireResource(true);

        taskList.add(preparingFormattedCurrDatetimeTask);
        taskList.add(prepareCreateCiSqlTask);
        taskList.add(createCiSubJobTask);// create cci sql
        taskList.add(createViewSubJobTask);// create view sql
        taskList.add(modifyTtlForBindArcTblSubTask);// modify ttl setting

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(taskList);
        executableDdlJob.getExcludeResources().add(sourceTableSchema);
        executableDdlJob.getExcludeResources().add(sourceTableName);
        executableDdlJob.getExcludeResources().add(targetTableSchema);
        executableDdlJob.getExcludeResources().add(targetTableName);

        return executableDdlJob;
    }

    //    boolean needIgnoreArchiveCciForCreateTableLike = false;
//    boolean needConvertToCreateArcCci = false;
    protected void checkIfAllowedCreatingTableLikeTtlDefinedTable(
        SqlCreateTable sqlCreateTable,
        ExecutionContext executionContext,
        Boolean[] needConvertToCreateArcCciOutput,
        Boolean[] needIgnoreArchiveCciForCreateTableLikeOutput
    ) {
        boolean createTblLikeTtlDefinedTbl =
            TtlUtil.checkIfCreateArcTblLikeRowLevelTtl(sqlCreateTable, executionContext);
        boolean useArchiveMode = sqlCreateTable.getArchiveMode() == ArchiveMode.TTL;
        boolean specifyColumnarEngine = sqlCreateTable.getEngine() == Engine.COLUMNAR;

        boolean needConvertToCreateArcCci = false;
        boolean needIgnoreArchiveCciForCreateTableLike = false;
        if (createTblLikeTtlDefinedTbl) {

            if (useArchiveMode && specifyColumnarEngine) {
                /**
                 * case 1:
                 * Convert
                 * "create table arc_tbl like ttl_tbl engine='columnar' archive_mode='ttl'"
                 * to
                 * "Create Cluster Columnar Index ci on ttl_tbl(ttl_col)"
                 */
                needConvertToCreateArcCci = true;
            } else {
                /**
                 * case 2:
                 * "create table normal_tbl like ttl_tbl engine='columnar'"
                 * or
                 * "create table normal_tbl like ttl_tbl archive_mode='ttl'"
                 * or
                 * "create table normal_tbl like ttl_tbl engine='oss' archive_mode='ttl'" (local partition)
                 * or
                 * "create table normal_tbl like ttl_tbl ..."
                 */

                if (useArchiveMode || specifyColumnarEngine) {
                    /**
                     * case 2-1:
                     * "create table normal_tbl like ttl_tbl engine='columnar'"
                     * or
                     * case 2-2:
                     * "create table normal_tbl like ttl_tbl archive_mode='ttl'"
                     * case 2-3:
                     * "create table normal_tbl like ttl_tbl engine='oss' archive_mode='ttl'" (local partition)
                     */
                    throw GeneralUtil.nestedException(
                        String.format(
                            "failed to create archive table for a ttl-defined table without specifying both engine=%s and archive_mode='%s'",
                            Engine.COLUMNAR, ArchiveMode.TTL));
                } else {
                    /**
                     * or
                     * case 2-4:
                     * "create table normal_tbl like ttl_tbl ..."
                     */
                    boolean allowCreateTblLikeIgnoreTtlDef = executionContext.getParamManager()
                        .getBoolean(ConnectionParams.ALLOW_CREATE_TABLE_LIKE_IGNORE_ARCHIVE_CCI);
                    TtlDefinitionInfo ttlInfo =
                        TtlUtil.fetchTtlInfoFromCreateTableLikeAst(sqlCreateTable, executionContext);
                    if (ttlInfo.needPerformExpiredDataArchiving()) {
                        if (!allowCreateTblLikeIgnoreTtlDef) {
                            throw GeneralUtil.nestedException(
                                "cannot create table like a ttl-defined table, please use the hint /*TDDL:cmd_extra(ALLOW_CREATE_TABLE_LIKE_IGNORE_ARCHIVE_CCI=true)*/ to do table creating");
                        } else {
                            needIgnoreArchiveCciForCreateTableLike = true;
                        }
                    }
                }

            }
        } else {
            /**
             * case 4:
             * "create table normal_tbl like non_ttl_tbl engine='columnar'"
             * or
             * "create table normal_tbl like non_ttl_tbl archive_mode='ttl'"
             * or
             * "create table normal_tbl like non_ttl_tbl engine='oss' archive_mode='ttl'" (local partition)
             * or
             * "create table normal_tbl like non_ttl_tbl ..."
             */

            if (specifyColumnarEngine) {
                /**
                 * case 3-1:
                 * "create table normal_tbl like non_ttl_tbl engine='columnar' archive_mode='ttl'"
                 * or
                 * case 3-2:
                 * "create table normal_tbl like non_ttl_tbl engine='columnar'"
                 */
                throw GeneralUtil.nestedException(
                    String.format(
                        "failed to create table like with engine='%s' archive_mode='%s' because the source table is not a ttl-defined table",
                        Engine.COLUMNAR, ArchiveMode.TTL));
            } else {
                /**
                 * Ignore handling
                 */
            }
        }
        if (needConvertToCreateArcCciOutput != null && needConvertToCreateArcCciOutput.length > 0) {
            needConvertToCreateArcCciOutput[0] = needConvertToCreateArcCci;
        }

        if (needIgnoreArchiveCciForCreateTableLikeOutput != null
            && needIgnoreArchiveCciForCreateTableLikeOutput.length > 0) {
            needIgnoreArchiveCciForCreateTableLikeOutput[0] = needIgnoreArchiveCciForCreateTableLike;
        }
    }

}
