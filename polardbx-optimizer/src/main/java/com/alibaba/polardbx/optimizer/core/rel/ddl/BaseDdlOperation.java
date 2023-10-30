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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseDdlOperation extends BaseQueryOperation {

    public DDL relDdl;
    protected String tableName;
    protected List<String> tableNameList;
    private String phyTable;

    protected Map<String, List<List<String>>> targetTablesHintCache;

    public BaseDdlOperation(RelOptCluster cluster, RelTraitSet traitSet, SqlDdl sqlDdl, RelDataType rowType) {
        super(cluster, traitSet, BytesSql.getBytesSql(RelUtils.toNativeSqlLine(sqlDdl)), sqlDdl,
            DbType.MYSQL);
        this.rowType = rowType;
        this.cursorMeta = CursorMeta.build(CalciteUtils.buildColumnMeta(rowType, "Ddl"));
    }

    protected BaseDdlOperation(RelOptCluster cluster, RelTraitSet traits, DDL ddl) {
        this(cluster, traits, (SqlDdl) ddl.getSqlNode(), ddl.getRowType());
        this.relDdl = ddl;
    }

    public BaseDdlOperation(DDL ddl) {
        this(ddl.getCluster(), ddl.getTraitSet(), ddl);
        if (CollectionUtils.isNotEmpty(ddl.getTableNameList())) {
            this.tableNameList = new ArrayList<>();
            for (SqlNode sqlNode : ddl.getTableNameList()) {
                final SqlIdentifier tableName = (SqlIdentifier) sqlNode;

                if (tableName.isSimple()) {
                    String dbName = PlannerContext.getPlannerContext(ddl).getSchemaName();
                    if (this.schemaName == null) {
                        this.schemaName = dbName;
                    } else if (!StringUtils.equalsIgnoreCase(dbName, this.schemaName)) {
                        throw new TddlNestableRuntimeException("DDL across databases are not supported yet.");
                    }
                    if (this.tableName == null) {
                        this.tableName = tableName.getSimple();
                    }
                    this.tableNameList.add(tableName.getSimple());
                } else {
                    final String dbName = tableName.names.get(0);
                    if (OptimizerContext.getContext(dbName) == null) {
                        throw new TddlNestableRuntimeException("Unknown database " + dbName);
                    }
                    if (this.schemaName == null) {
                        this.schemaName = dbName;
                    } else if (!StringUtils.equalsIgnoreCase(dbName, this.schemaName)) {
                        throw new TddlNestableRuntimeException("DDL across databases are not supported yet.");
                    }
                    if (this.tableName == null) {
                        this.tableName = Util.last(tableName.names);
                    }
                    this.tableNameList.add(Util.last(tableName.names));
                }
            }
        } else {
            final SqlIdentifier tableName = (SqlIdentifier) ddl.getTableName();
            if (tableName.isSimple()) {
                this.tableName = tableName.getSimple();
                this.schemaName = PlannerContext.getPlannerContext(ddl).getSchemaName();
            } else {
                final String schemaName = tableName.names.get(0);
                if (OptimizerContext.getContext(schemaName) == null) {
                    throw new TddlNestableRuntimeException("Unknown database " + schemaName);
                }
                this.tableName = Util.last(tableName.names);
                this.schemaName = schemaName;
            }
        }

        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = DefaultSchema.getSchemaName();
        }
    }

    public BaseDdlOperation(DDL ddl, List<SqlIdentifier> objectNames) {
        this(ddl.getCluster(), ddl.getTraitSet(), ddl);
        if (AlterTablePartitionHelper.checkIfFromAlterIndexPartition(ddl)) {
            SqlIdentifier gsiFullTblNameId =
                (SqlIdentifier) AlterTablePartitionHelper.fetchGsiFullTableNameAstForAlterIndexPartition(ddl);
            if (gsiFullTblNameId != null) {
                AlterTable alterTbl = (AlterTable) ddl;
                List<String> gsiFullTblNames = gsiFullTblNameId.names;
                this.schemaName = gsiFullTblNames.get(0);
                this.tableName = gsiFullTblNames.get(1);
                alterTbl.setTableName(gsiFullTblNameId);
                return;
            }
        }

        int nameHierarchy = objectNames.size();
        if (nameHierarchy > 3) {
            throw new TddlNestableRuntimeException("Invalid table:" + tableName);
        }
        this.tableName = objectNames.get(0).getSimple();
        if (nameHierarchy == 1) {
            this.schemaName = PlannerContext.getPlannerContext(ddl).getSchemaName();
        } else if (nameHierarchy == 2) {
            String schemaOrTable = objectNames.get(1).getSimple();
            SchemaManager schemaManager =
                OptimizerContext.getContext(DefaultSchema.getSchemaName()).getLatestSchemaManager();
            boolean throwEx = false;
            try {
                TableMeta tableMeta = schemaManager.getTable(schemaOrTable);
                if (tableMeta.withGsi()) {
                    String tableNameStr = tableMeta.getGsiTableMetaBean().indexMap.keySet().stream()
                        .filter(idx -> TddlSqlToRelConverter.unwrapGsiName(idx).equalsIgnoreCase(this.tableName))
                        .findFirst().orElse(null);
                    if (tableNameStr == null) {
                        throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, this.tableName);
                    }
                    throwEx = true;
                    this.tableName = tableNameStr;
                    ddl.setTableName(new SqlIdentifier(this.tableName, SqlParserPos.ZERO));
                } else {
                    throwEx = true;
                    if (OptimizerContext.getContext(schemaOrTable) == null) {
                        throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, this.tableName);
                    } else {
                        schemaManager = OptimizerContext.getContext(schemaOrTable).getLatestSchemaManager();
                        schemaManager.getTable(this.tableName);
                        this.schemaName = schemaOrTable;
                    }
                }
            } catch (Exception ex) {
                if (throwEx) {
                    throw ex;
                }
                if (OptimizerContext.getContext(schemaOrTable) == null) {
                    throw new TddlNestableRuntimeException("Unknown database " + schemaOrTable);
                } else {
                    schemaManager = OptimizerContext.getContext(schemaOrTable).getLatestSchemaManager();
                    schemaManager.getTable(this.tableName);
                    this.schemaName = schemaOrTable;
                }
            }
        } else if (nameHierarchy == 3) {
            String thisSchema = objectNames.get(2).getSimple();
            if (OptimizerContext.getContext(thisSchema) == null) {
                throw new TddlNestableRuntimeException("Unknown database " + thisSchema);
            }
            SchemaManager schemaManager = OptimizerContext.getContext(thisSchema).getLatestSchemaManager();
            String thisTable = objectNames.get(1).getSimple();
            TableMeta tableMeta = schemaManager.getTable(thisTable);
            if (tableMeta.withGsi()) {
                this.tableName = tableMeta.getGsiTableMetaBean().indexMap.keySet().stream()
                    .filter(idx -> TddlSqlToRelConverter.unwrapGsiName(idx).equalsIgnoreCase(this.tableName))
                    .findFirst().orElse(null);
                ddl.setTableName(new SqlIdentifier(this.tableName, SqlParserPos.ZERO));
            } else {
                throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, this.tableName);
            }
            this.schemaName = thisSchema;
        }
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = DefaultSchema.getSchemaName();
        }
    }

    @Override
    public String getSchemaName() {
        return this.schemaName;
    }

    @Override
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPhyTable() {
        return phyTable;
    }

    public void setPhyTable(String phyTable) {
        this.phyTable = phyTable;
    }

    public SqlNode getTableNameNode() {
        return relDdl.getTableName();
    }

    @Override
    public SqlKind getKind() {
        return relDdl.sqlNode.getKind();
    }

    // TODO implement it in subclass
    public DdlType getDdlType() {
        switch (getKind()) {
        case CREATE_TABLE:
            return DdlType.CREATE_TABLE;
        case ALTER_TABLE:
            return DdlType.ALTER_TABLE;
        case RENAME_TABLE:
            return DdlType.RENAME_TABLE;
        case TRUNCATE_TABLE:
            return DdlType.TRUNCATE_TABLE;
        case DROP_TABLE:
            return DdlType.DROP_TABLE;
        case CREATE_INDEX:
            return DdlType.CREATE_INDEX;
        case DROP_INDEX:
            return DdlType.DROP_INDEX;
        case CHECK_GLOBAL_INDEX:
            return DdlType.CHECK_GLOBAL_INDEX;
        case MOVE_DATABASE:
            return DdlType.MOVE_DATABASE;
        case UNARCHIVE:
            return DdlType.UNARCHIVE;
        case ALTER_TABLEGROUP:
            return DdlType.ALTER_TABLEGROUP;
        case REBALANCE:
            return DdlType.REBALANCE;
        case REFRESH_TOPOLOGY:
            return DdlType.REFRESH_TOPOLOGY;
        case CREATE_FUNCTION:
            return DdlType.CREATE_FUNCTION;
        case DROP_FUNCTION:
            return DdlType.DROP_FUNCTION;
        case ALTER_FUNCTION:
            return DdlType.ALTER_FUNCTION;
        case CREATE_JAVA_FUNCTION:
            return DdlType.CREATE_JAVA_FUNCTION;
        case DROP_JAVA_FUNCTION:
            return DdlType.DROP_JAVA_FUNCTION;
        case CREATE_PROCEDURE:
            return DdlType.CREATE_PROCEDURE;
        case DROP_PROCEDURE:
            return DdlType.DROP_PROCEDURE;
        case ALTER_PROCEDURE:
            return DdlType.ALTER_PROCEDURE;
        case CREATE_DATABASE:
            return DdlType.CREATE_DATABASE_LIKE_AS;
        case PUSH_DOWN_UDF:
            return DdlType.PUSH_DOWN_UDF;
        case ALTER_TABLE_SET_TABLEGROUP:
            return DdlType.ALTER_TABLE_SET_TABLEGROUP;
        case ALTER_TABLEGROUP_ADD_TABLE:
            return DdlType.ALTER_TABLEGROUP_ADD_TABLE;
        case MERGE_TABLEGROUP:
            return DdlType.MERGE_TABLEGROUP;
        default:
            return DdlType.UNSUPPORTED;
        }
    }

    public Map<String, List<List<String>>> getTargetTablesHintCache() {
        return targetTablesHintCache;
    }

    public void setTargetTablesHintCache(Map<String, List<List<String>>> targetTablesHintCache) {
        this.targetTablesHintCache = targetTablesHintCache;
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           ExecutionContext executionContext) {
        param = new HashMap<>();
        return Pair.of("", param);
    }

    @Override
    public SqlNodeList getHints() {
        return ((SqlDdl) relDdl.sqlNode).getHints();
    }

    @Override
    public RelNode setHints(SqlNodeList hints) {
        ((SqlDdl) relDdl.sqlNode).setHints(hints);
        return this;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, getExplainName());
        pw.item("sql", this.bytesSql.display());
        return pw;
    }

    @Override
    protected String getExplainName() {
        return "LogicalDdl";
    }

    @Override
    protected RelDataType deriveRowType() {
        return relDdl.getRowType();
    }

    /**
     * Check if ddl operation involves file storage.
     * 1. ddl on a file storage table, and the table is bound to an innodb table, reject the ddl
     * 2. ddl on a file storage table, and the table is not bound to any innodb table,
     * check whether the ddl is portal to file storage
     *
     * @return true if the ddl can be executed
     */
    public boolean isSupportedByFileStorage() {
        return false;
    }

    /**
     * Check if ddl operation involve file storage.
     * 1. ddl on an innodb table, and the table is NOT binding to a file storage table,
     * 2. ddl on an innodb table, and the table is binding to a file storage table,
     * check whether the ddl is portal to file storage
     *
     * @return true if the ddl can be executed
     */
    public boolean isSupportedByBindFileStorage() {
        return false;
    }

    /**
     * Check if ddl operation on file storage
     *
     * @return true if ddl on file storage
     */
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        String schemaName = getSchemaName();
        String logicalTableName = getTableName();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTableWithNull(logicalTableName);
        if (tableMeta == null) {
            return false;
        }
        return Engine.isFileStore(tableMeta.getEngine());
    }

    /**
     * Check if ddl operation on innodb binding to file storage
     *
     * @return true if ddl on innodb binding to file storage
     */
    public boolean checkIfBindFileStorage(ExecutionContext executionContext) {
        String schemaName = getSchemaName();
        String logicalTableName = getTableName();
        return !CheckOSSArchiveUtil.checkWithoutOSS(schemaName, logicalTableName);
    }
}
