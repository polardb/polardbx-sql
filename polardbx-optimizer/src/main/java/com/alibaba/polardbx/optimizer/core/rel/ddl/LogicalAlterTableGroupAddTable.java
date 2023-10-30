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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupAddTablePreparedData;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupAddTable;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupAddTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class LogicalAlterTableGroupAddTable extends BaseDdlOperation {

    private AlterTableGroupAddTablePreparedData preparedData;

    public LogicalAlterTableGroupAddTable(DDL ddl) {
        super(ddl);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        if (!CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, preparedData.getTableGroupName())) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
                "unarchive tablegroup " + preparedData.getTableGroupName());
        }
        String schemaName = preparedData.getSchemaName();
        for (String tableName : preparedData.getTables()) {
            if (!CheckOSSArchiveUtil.checkWithoutOSS(schemaName, tableName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
                    "unarchive table " + schemaName + "." + tableName);
            }
        }
        return false;
    }

    public void preparedData(ExecutionContext ec) {
        AlterTableGroupAddTable alterTableGroupAddTable = (AlterTableGroupAddTable) relDdl;
        String tableGroupName = alterTableGroupAddTable.getTableGroupName();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupAddTable.getAst();
        SqlAlterTableGroupAddTable sqlAlterTableGroupAddTable =
            (SqlAlterTableGroupAddTable) sqlAlterTableGroup.getAlters().get(0);

        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        boolean isEmptyGroup = tableGroupConfig == null || GeneralUtil.isEmpty(tableGroupConfig.getAllTables());
        preparedData = new AlterTableGroupAddTablePreparedData();
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);

        SchemaManager schemaManager = ec.getSchemaManager(schemaName);
        Map<String, Long> tablesVersion = new TreeMap<>(String::compareToIgnoreCase);
        Set<String> tables = new TreeSet<>(String::compareToIgnoreCase);
        for (SqlNode sqlNode : sqlAlterTableGroupAddTable.getTables()) {
            SqlIdentifier tableNameIdentifier = (SqlIdentifier) (sqlNode);
            String tableName;
            if (tableNameIdentifier.names != null && tableNameIdentifier.names.size() > 2) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_NAME_TOO_MANY_HIERARCHY,
                    "the hierarchy of table name can't more than 2 here");
            }
            if (tableNameIdentifier.isSimple()) {
                tableName =
                    Util.last(tableNameIdentifier.names);
            } else {
                tableName = tableNameIdentifier.names.get(0);
                String indexName = tableNameIdentifier.getLastName();
                TableMeta tableMeta = schemaManager.getTable(tableName);
                if (tableMeta.withGsi()) {
                    String tableNameStr = tableMeta.getGsiTableMetaBean().indexMap.keySet().stream()
                        .filter(idx -> TddlSqlToRelConverter.unwrapGsiName(idx).equalsIgnoreCase(indexName))
                        .findFirst().orElse(null);
                    if (tableNameStr == null) {
                        throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, indexName);
                    }
                    tableName = tableNameStr;
                }
            }
            tables.add(tableName);
            if (isEmptyGroup && preparedData.getReferenceTable() == null) {
                preparedData.setReferenceTable(tableName);
            }
            TableMeta tableMeta = schemaManager.getTable(tableName);
            String primaryTableName = tableName;
            if (tableMeta.isGsi()) {
                //all the gsi table version change will be behavior by primary table
                assert
                    tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(primaryTableName);
            }
            tablesVersion.put(primaryTableName, tableMeta.getVersion());
        }
        preparedData.setTables(tables);
        preparedData.setTableGroupConfig(tableGroupConfig);
        preparedData.setTableVersions(tablesVersion);
        preparedData.setForce(alterTableGroupAddTable.isForce());
    }

    public AlterTableGroupAddTablePreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableGroupAddTable create(DDL ddl) {
        return new LogicalAlterTableGroupAddTable(AlterTablePartitionHelper.fixAlterTableGroupDdlIfNeed(ddl));
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        // TODO(siyun): Redundant preparedData, consider to optimize it
        preparedData(executionContext);
        if (TableGroupNameUtil.isOssTg(preparedData.getTableGroupName())) {
            return true;
        }
        String schemaName = preparedData.getSchemaName();
        for (String tableName : preparedData.getTables()) {
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTableWithNull(tableName);
            if (Engine.isFileStore(tableMeta.getEngine())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean checkIfBindFileStorage(ExecutionContext executionContext) {
        preparedData(executionContext);
        if (!CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, preparedData.getTableGroupName())) {
            return true;
        }
        String schemaName = preparedData.getSchemaName();
        for (String tableName : preparedData.getTables()) {
            if (!CheckOSSArchiveUtil.checkWithoutOSS(schemaName, tableName)) {
                return true;
            }
        }
        return false;
    }
}
