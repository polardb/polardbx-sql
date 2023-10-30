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

import com.alibaba.polardbx.common.cdc.TablesExtInfo;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DdlPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameTablesPreparedData;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.TableTopologyUtil;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.rel.ddl.RenameTables;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.common.cdc.TableMode.PARTITION;
import static com.alibaba.polardbx.common.cdc.TableMode.SHARDING;

public class LogicalRenameTables extends BaseDdlOperation {

    private RenameTablesPreparedData renameTablesPreparedData;

    public LogicalRenameTables(RenameTables renameTables) {
        super(renameTables);
    }

    public static LogicalRenameTables create(RenameTables renameTables) {
        return new LogicalRenameTables(renameTables);
    }

    public RenameTablesPreparedData getRenameTablesPreparedData() {
        return renameTablesPreparedData;
    }

    public void prepareData() {
        renameTablesPreparedData = preparePrimaryData();
    }

    private RenameTablesPreparedData preparePrimaryData() {
        RenameTablesPreparedData renameTablesPreparedData = new RenameTablesPreparedData();
        List<String> fromTableNames = new ArrayList<>();
        List<String> toTableNames = new ArrayList<>();
        List<String> collate = new ArrayList<>();
        List<TablesExtInfo> cdcMetas = new ArrayList<>();
        List<DdlPreparedData> oldTableNamePrepareDataList = new ArrayList<>();
        List<Map<String, Set<String>>> newTableTopologies = new ArrayList<>();

        Set<String> names = new TreeSet<>(String::compareToIgnoreCase);
        Map<String, String> sourceTableMap = new HashMap<>();

        Set<String> leftTableNames = new TreeSet<>(String::compareToIgnoreCase);
        Set<String> rightTableNames = new TreeSet<>(String::compareToIgnoreCase);
        Set<String> realSourceTables = new TreeSet<>(String::compareToIgnoreCase);

        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        for (int i = 0; i < tableNameList.size(); ++i) {
            String tbName = tableNameList.get(i);

            SqlIdentifier newTableName = (SqlIdentifier) relDdl.getNewTableNameList().get(i);
            if (newTableName != null && !newTableName.isSimple()) {
                String targetSchema = newTableName.names.get(0);
                if (OptimizerContext.getContext(targetSchema) == null) {
                    throw new TddlNestableRuntimeException("Unknown target database " + targetSchema);
                } else if (!TStringUtil.equalsIgnoreCase(targetSchema, schemaName)) {
                    throw new TddlNestableRuntimeException("Target database must be the same as source database");
                }
            }

            fromTableNames.add(tbName);
            toTableNames.add(newTableName.getLastName());
            boolean mustRealTable = false;
            if (!leftTableNames.contains(tbName) && !rightTableNames.contains(tbName)) {
                realSourceTables.add(tbName);
                mustRealTable = true;
            }
            leftTableNames.add(tbName);
            rightTableNames.add(newTableName.getLastName());

            TableMeta sourceTableMeta = null;
            if (sourceTableMap.containsKey(tbName.toLowerCase())) {
                String sourceTable = sourceTableMap.get(tbName.toLowerCase());
                sourceTableMap.put(newTableName.getLastName().toLowerCase(), sourceTable);
                sourceTableMeta = sm.getTable(sourceTable);
            } else {
                sourceTableMap.put(newTableName.getLastName().toLowerCase(), tbName.toLowerCase());
                sourceTableMeta = sm.getTable(tbName);
            }
            collate.add(sourceTableMeta.getDefaultCollation());
            cdcMetas.add(buildCdcMeta(sourceTableMeta));
            newTableTopologies.add(buildTopology(sourceTableMeta));

            names.add(tbName);
            names.add(newTableName.getLastName());

            if (mustRealTable) {
                TableMeta tableMeta = sm.getTable(tbName);
                DdlPreparedData old = new DdlPreparedData();
                old.setSchemaName(schemaName);
                old.setTableName(tbName);
                old.setTableVersion(tableMeta.getVersion());
                oldTableNamePrepareDataList.add(old);
            }
        }

        List<String> distinctNames = new ArrayList<>(names);

        renameTablesPreparedData.setSchemaName(schemaName);
        renameTablesPreparedData.setFromTableNames(fromTableNames);
        renameTablesPreparedData.setToTableNames(toTableNames);
        renameTablesPreparedData.setCollate(collate);
        renameTablesPreparedData.setCdcMetas(cdcMetas);
        renameTablesPreparedData.setDistinctNames(distinctNames);
        renameTablesPreparedData.setOldTableNamePrepareDataList(oldTableNamePrepareDataList);
        renameTablesPreparedData.setNewTableTopologies(newTableTopologies);
        renameTablesPreparedData.setRealSourceTables(realSourceTables);

        return renameTablesPreparedData;
    }

    private TablesExtInfo buildCdcMeta(TableMeta tableMeta) {
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            return new TablesExtInfo(PARTITION, tableMeta.getPartitionInfo().getTableType().getTableTypeIntValue());
        } else {
            return new TablesExtInfo(SHARDING, buildDrdsTableType(tableMeta));
        }
    }

    private int buildDrdsTableType(TableMeta tableMeta) {
        boolean isSharding = TableTopologyUtil.isShard(tableMeta);
        boolean isGsi = tableMeta.isGsi();
        boolean isBroadCast = TableTopologyUtil.isBroadcast(tableMeta);
        if (isGsi) {
            return GsiMetaManager.TableType.GSI.getValue();
        } else if (isSharding) {
            return GsiMetaManager.TableType.SHARDING.getValue();
        } else if (isBroadCast) {
            return GsiMetaManager.TableType.BROADCAST.getValue();
        } else {
            return GsiMetaManager.TableType.SINGLE.getValue();
        }
    }

    private Map<String, Set<String>> buildTopology(TableMeta tableMeta) {
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            return tableMeta.getPartitionInfo().getTopology();
        } else {
            String tbName = tableMeta.getTableName();
            TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
            TableRule tableRule = tddlRuleManager.getTableRule(tbName);
            return new HashMap<>(tableRule.getActualTopology());
        }
    }
}
