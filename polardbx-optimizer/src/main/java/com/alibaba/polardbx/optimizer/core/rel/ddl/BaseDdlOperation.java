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
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseDdlOperation extends BaseQueryOperation {

    public DDL relDdl;
    protected String tableName;
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
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = DefaultSchema.getSchemaName();
        }
    }

    public BaseDdlOperation(DDL ddl, List<SqlIdentifier> objectNames) {
        this(ddl.getCluster(), ddl.getTraitSet(), ddl);
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
        case CREATE_PROCEDURE:
            return DdlType.CREATE_PROCEDURE;
        case DROP_PROCEDURE:
            return DdlType.DROP_PROCEDURE;
        case ALTER_PROCEDURE:
            return DdlType.ALTER_PROCEDURE;
        case PUSH_DOWN_UDF:
            return DdlType.PUSH_DOWN_UDF;
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

}
