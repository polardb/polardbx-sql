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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.UnArchivePreparedData;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.UnArchive;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlUnArchive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Shi Yuxuan
 */
public class LogicalUnArchive extends BaseDdlOperation {
    private String schemaName;
    private UnArchivePreparedData preparedData;

    public LogicalUnArchive(DDL ddl) {
        super(ddl.getCluster(), ddl.getTraitSet(), ddl);
    }

    public void preparedData() {
        UnArchive unArchive = (UnArchive) relDdl;
        SqlUnArchive sqlUnArchive = (SqlUnArchive) unArchive.sqlNode;
        Map<String, Long> tables = new TreeMap<>(String::compareToIgnoreCase);
        Preconditions.checkArgument(DbInfoManager.getInstance().isNewPartitionDb(getSchemaName()),
            "Database: " + getSchemaName() + " should be in auto mode");
        SchemaManager sm = OptimizerContext.getContext(getSchemaName()).getLatestSchemaManager();
        // build and check existence of all tables
        try (Connection conn = MetaDbDataSource.getInstance().getDataSource().getConnection()) {
            switch (sqlUnArchive.getTarget()) {
            case TABLE: {
                SqlIdentifier table = (SqlIdentifier) sqlUnArchive.getNode();
                String targetTable = table.getLastName();
                // check table exists
                Preconditions.checkArgument(!StringUtils.isEmpty(targetTable), "table name is null");
                ResultSet rs = conn.prepareStatement(
                        String.format("select count(1) from %s where table_schema=\"%s\" and table_name=\"%s\"",
                            GmsSystemTables.TABLES, getSchemaName(), table.getLastName()))
                    .executeQuery();
                rs.next();
                Preconditions.checkArgument(rs.getLong(1) == 1,
                    "Table:" + getSchemaName() + "." + targetTable + " doesn't exist");
                tables.put(targetTable, sm.getTable(targetTable).getVersion());
                preparedData = new UnArchivePreparedData(getSchemaName(), retainUsefulTables(tables, conn), null);
                break;
            }
            case TABLE_GROUP: {
                Preconditions.checkArgument(((SqlIdentifier) sqlUnArchive.getNode()).isSimple(),
                    "Unknown tablegroup: " + sqlUnArchive.getNode().toString());
                String tableGroup = ((SqlIdentifier) sqlUnArchive.getNode()).getLastName();

                final TableGroupInfoManager tableGroupInfoManager =
                    OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager();
                TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroup);
                if (tableGroupConfig == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                        "Tablegroup: " + tableGroup + " doesn't exists");
                }
                for (String targetTable : tableGroupConfig.getAllTables()) {
                    tables.put(targetTable, sm.getTable(targetTable).getVersion());
                }
                Preconditions.checkArgument(tables.size() > 0,
                    "Tablegroup: " + tableGroup + " has no table");
                preparedData = new UnArchivePreparedData(getSchemaName(), retainUsefulTables(tables, conn), tableGroup);
                break;
            }
            case DATABASE: {
                Preconditions.checkArgument(TableInfoManager.getSchemaDefaultDbIndex(getSchemaName()) != null,
                    "Database: " + getSchemaName() + " doesn't exists");

                ResultSet rs = conn.prepareStatement(
                        String.format("select table_name from %s where table_schema=\"%s\"",
                            GmsSystemTables.TABLES, getSchemaName()))
                    .executeQuery();
                while (rs.next()) {
                    String targetTable = rs.getString(1);
                    tables.put(targetTable, sm.getTable(targetTable).getVersion());
                }
                Preconditions.checkArgument(tables.size() > 0,
                    "Database: " + getSchemaName() + " has no table");
                preparedData = new UnArchivePreparedData(getSchemaName(), retainUsefulTables(tables, conn), null);
                break;
            }
            default:
                preparedData = new UnArchivePreparedData(getSchemaName(), tables, null);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw GeneralUtil.nestedException(e);
        }
    }

    public UnArchivePreparedData getPreparedData() {
        return preparedData;
    }

    /**
     * retain all local partition tables with archive tables
     *
     * @param tables tables to be tested
     * @param conn gms connection
     * @return tables to be unarchived
     */
    private Map<String, Long> retainUsefulTables(Map<String, Long> tables, Connection conn) {
        Map<String, Long> results = new TreeMap<>(String::compareToIgnoreCase);
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(conn);
        for (Map.Entry<String, Long> entry : tables.entrySet()) {
            TableLocalPartitionRecord record =
                tableInfoManager.getLocalPartitionRecord(getSchemaName(), entry.getKey());
            // not local partition
            if (record == null) {
                continue;
            }

            // without archive table
            if (StringUtils.isEmpty(record.getArchiveTableName()) || StringUtils.isEmpty(
                record.getArchiveTableName())) {
                continue;
            }
            results.put(entry.getKey(), entry.getValue());
        }
        return results;
    }

    @Override
    public String getSchemaName() {
        if (schemaName == null) {
            schemaName = ((SqlUnArchive) nativeSqlNode).getSchemaName();
            if (StringUtils.isEmpty(schemaName)) {
                schemaName = PlannerContext.getPlannerContext(this.relDdl).getSchemaName();
            }
            if (StringUtils.isEmpty(schemaName)) {
                schemaName = DefaultSchema.getSchemaName();
            }
            if (schemaName == null) {
                schemaName = "";
            }
            Preconditions.checkArgument(TableInfoManager.getSchemaDefaultDbIndex(schemaName) != null,
                "Database: " + schemaName + " doesn't exists");
        }
        return schemaName;
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    public static LogicalUnArchive create(DDL input) {
        return new LogicalUnArchive(input);
    }
}
