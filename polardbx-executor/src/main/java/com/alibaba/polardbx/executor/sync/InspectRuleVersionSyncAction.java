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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gms.TableRuleManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.topology.ServerInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.rule.TableRule;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.rule.database.util.TddlRuleParam.CONSISTENCY_CONTENT_DIFF;
import static com.alibaba.polardbx.rule.database.util.TddlRuleParam.CONSISTENCY_LEADER;
import static com.alibaba.polardbx.rule.database.util.TddlRuleParam.CONSISTENCY_READ_ONLY;
import static com.alibaba.polardbx.rule.database.util.TddlRuleParam.CONSISTENCY_SOURCE;
import static com.alibaba.polardbx.rule.database.util.TddlRuleParam.CONSISTENCY_TABLE_COUNT;

/**
 * Created by chensr on 2017/4/28.
 */
public class InspectRuleVersionSyncAction implements ISyncAction {

    private String schemaName = null;

    public InspectRuleVersionSyncAction() {
    }

    public InspectRuleVersionSyncAction(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("RULE_INFO");
        handleGMS(resultCursor);
        return resultCursor;
    }

    private void handleGMS(ArrayResultCursor resultCursor) {
        resultCursor.addColumn(CONSISTENCY_SOURCE, DataTypes.StringType);
        resultCursor.addColumn(CONSISTENCY_TABLE_COUNT, DataTypes.StringType);
        resultCursor.addColumn(CONSISTENCY_CONTENT_DIFF, DataTypes.StringType);
        resultCursor.addColumn(CONSISTENCY_LEADER, DataTypes.StringType);
        resultCursor.addColumn(CONSISTENCY_READ_ONLY, DataTypes.StringType);
        resultCursor.initMeta();

        String schemaName = OptimizerContext.getContext(this.schemaName).getSchemaName();
        String serverInfo = TddlNode.getNodeInfo();
        String isLeader = ExecUtils.hasLeadership(schemaName) ? "Y" : "";

        GmsNodeManager.GmsNode localNode = GmsNodeManager.getInstance().getLocalNode();

        String isReadOnly = "";
        if ((localNode != null && localNode.instType != ServerInfoRecord.INST_TYPE_MASTER) ||
            (localNode == null && ConfigDataMode.isSlaveMode())) {
            isReadOnly = "Y";
        }

        try {
            List<TablesExtRecord> records;
            TableInfoManager tableInfoManager = new TableInfoManager();
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                tableInfoManager.setConnection(metaDbConn);
                records = tableInfoManager.queryTableExts(schemaName);
            } finally {
                tableInfoManager.setConnection(null);
            }

            Map<String, TablesExtRecord> tableRulesInMetaDB = new HashMap<>();
            if (records != null && records.size() > 0) {
                for (TablesExtRecord record : records) {
                    tableRulesInMetaDB.put(record.tableName, record);
                }
            }

            Map<String, TableRule> tableRulesInMemory = TableRuleManager.getTableRules(schemaName);

            // Tables in both Meta DB and Memory, but different.
            Set<String> tablesDifferent = new HashSet<>();

            // Tables in Meta DB, but not in memory.
            Set<String> tablesInMetaDBOnly = new HashSet<>();
            for (String tableInMetaDB : tableRulesInMetaDB.keySet()) {
                if (!tableRulesInMemory.keySet().contains(tableInMetaDB)) {
                    tablesInMetaDBOnly.add(tableInMetaDB);
                    continue;
                }
                if (!compareTableRule(tableRulesInMetaDB.get(tableInMetaDB), tableRulesInMemory.get(tableInMetaDB))) {
                    tablesDifferent.add(tableInMetaDB);
                }
            }

            // Tables in Memory, but not in Meta DB.
            Set<String> tablesInMemoryOnly = new HashSet<>();
            for (String tableInMemory : tableRulesInMemory.keySet()) {
                if (!tableRulesInMetaDB.keySet().contains(tableInMemory)) {
                    tablesInMemoryOnly.add(tableInMemory);
                    continue;
                }
                if (!compareTableRule(tableRulesInMetaDB.get(tableInMemory), tableRulesInMemory.get(tableInMemory))) {
                    tablesDifferent.add(tableInMemory);
                }
            }

            buildResult(tableRulesInMemory.size(), tablesInMetaDBOnly, tablesInMemoryOnly, tablesDifferent,
                resultCursor, serverInfo, isLeader, isReadOnly);

        } catch (Exception e) {
            resultCursor
                .addRow(new Object[] {
                    serverInfo, 0, e instanceof NullPointerException ? "NPE" : e.getMessage(), isLeader, isReadOnly});
        }
    }

    private void buildResult(int size, Set<String> tablesInMetaDBOnly, Set<String> tablesInMemoryOnly,
                             Set<String> tablesDifferent, ArrayResultCursor resultCursor, String serverInfo,
                             String isLeader, String isReadOnly) {
        String diff = "";
        if (tablesInMetaDBOnly != null && tablesInMetaDBOnly.size() > 0) {
            diff = "- " + concat(tablesInMetaDBOnly);
            resultCursor.addRow(new Object[] {serverInfo, size, diff, isLeader, isReadOnly});
        }
        if (tablesInMemoryOnly != null && tablesInMemoryOnly.size() > 0) {
            diff = "+ " + concat(tablesInMemoryOnly);
            resultCursor.addRow(new Object[] {serverInfo, size, diff, isLeader, isReadOnly});
        }
        if (tablesDifferent != null && tablesDifferent.size() > 0) {
            diff = "! " + concat(tablesDifferent);
            resultCursor.addRow(new Object[] {serverInfo, size, diff, isLeader, isReadOnly});
        }
        if (TStringUtil.isBlank(diff)) {
            resultCursor.addRow(new Object[] {serverInfo, size, diff, isLeader, isReadOnly});
        }
    }

    private String concat(Set<String> tableNames) {
        return TStringUtil.join(tableNames.toArray(), ", ");
    }

    private boolean compareTableRule(TablesExtRecord tableRuleInMetaDB, TableRule tableRuleInMemory) {
        if (tableRuleInMetaDB == null && tableRuleInMemory == null) {
            return true;
        }
        if (tableRuleInMetaDB == null || tableRuleInMemory == null) {
            return false;
        }
        return compareRuleStr(tableRuleInMetaDB.dbNamePattern, tableRuleInMemory.getDbNamePattern())
            && compareRuleStr(tableRuleInMetaDB.tbNamePattern, tableRuleInMemory.getTbNamePattern())
            && compareRuleStr(tableRuleInMetaDB.dbRule, tableRuleInMemory.getDbRuleStrs())
            && compareRuleStr(tableRuleInMetaDB.tbRule, tableRuleInMemory.getTbRulesStrs())
            && (tableRuleInMetaDB.fullTableScan == (tableRuleInMemory.isAllowFullTableScan() ? 1 : 0))
            && (tableRuleInMetaDB.broadcast == (tableRuleInMemory.isBroadcast() ? 1 : 0));
    }

    private boolean compareRuleStr(String ruleStrInMetaDB, String[] ruleStrInMemory) {
        if (TStringUtil.isBlank(ruleStrInMetaDB)
            && (ruleStrInMemory == null || TStringUtil.isBlank(ruleStrInMemory[0]))) {
            return true;
        }
        if (TStringUtil.isBlank(ruleStrInMetaDB)
            || (ruleStrInMemory == null || TStringUtil.isBlank(ruleStrInMemory[0]))) {
            return false;
        }
        return TStringUtil.equals(ruleStrInMetaDB, ruleStrInMemory[0]);
    }

    private boolean compareRuleStr(String ruleStrInMetaDB, String ruleStrInMemory) {
        if (TStringUtil.isBlank(ruleStrInMetaDB) && TStringUtil.isBlank(ruleStrInMemory)) {
            return true;
        }
        if (TStringUtil.isBlank(ruleStrInMetaDB) || TStringUtil.isBlank(ruleStrInMemory)) {
            return false;
        }
        return TStringUtil.equals(ruleStrInMetaDB, ruleStrInMemory);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}
