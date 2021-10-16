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

package com.alibaba.polardbx.rule.gms;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.record.RecordConverter;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRuleConfig;
import com.alibaba.polardbx.rule.VirtualTableRoot;
import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TddlRuleGmsConfig extends TddlRuleConfig {

    protected static final Logger LOGGER = LoggerFactory.getLogger(TddlRuleGmsConfig.class);

    protected boolean available = true;

    protected String schemaName;

    @Override
    public void doInit() {
        initRules();
    }

    @Override
    public void doDestroy() {
    }

    private void initRules() {
        // Always use the default version for GMS.
        String version = TddlRuleConfig.NO_VERSION_NAME;

        VirtualTableRoot virtualTableRoot = new VirtualTableRoot();

        Map<String, TableRule> virtualTableMap = TreeMaps.caseInsensitiveMap();

        List<TablesExtRecord> records;
        TableInfoManager tableInfoManager = new TableInfoManager();

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            virtualTableRoot.setDefaultDbIndex(tableInfoManager.getDefaultDbIndex(schemaName));
            records = tableInfoManager.queryVisibleTableExts(schemaName);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }

        if (records != null && records.size() > 0) {
            for (TablesExtRecord record : records) {
                TableRule tableRule = initTableRule(record);
                virtualTableMap.put(record.tableName, tableRule);
            }
        }

        virtualTableRoot.setTableRules(virtualTableMap);

        virtualTableRoot.init();

        vtrs.put(version, virtualTableRoot);

        versionIndex = new HashMap<>(1);
        versionIndex.put(0, version);

        getVersionedTableNames().put(version, Maps.newConcurrentMap());
    }

    public void addRule(String tableName, boolean overridden) {
        synchronized (vtrs) {
            VirtualTableRoot virtualTableRoot = vtrs.get(versionIndex.get(0));
            Map<String, TableRule> virtualTableMap = virtualTableRoot.getTableRules();

            if (overridden || !virtualTableMap.keySet().contains(tableName)) {
                TableInfoManager tableInfoManager = new TableInfoManager();

                TablesExtRecord record;
                try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                    tableInfoManager.setConnection(metaDbConn);
                    // Get new table info
                    record = tableInfoManager.queryVisibleTableExt(schemaName, tableName, false);
                    if (record == null) {
                        // Check if there is an ongoing RENAME TABLE operation, so search with new table name.
                        record = tableInfoManager.queryVisibleTableExt(schemaName, tableName, true);
                    }
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
                } finally {
                    tableInfoManager.setConnection(null);
                }

                if (record != null) {
                    TableRule currentRule = virtualTableMap.get(tableName);
                    if (currentRule != null && record.version == currentRule.getVersion()) {
                        return;
                    }
                    // Add new table rule
                    TableRule tableRule = initTableRule(record);
                    virtualTableMap.put(tableName, tableRule);

                    // Remove current table names and regenerate when next call.
                    getVersionedTableNames().remove(versionIndex.get(0));
                }
            }
        }
    }

    public void removeRule(String tableName) {
        synchronized (vtrs) {
            VirtualTableRoot virtualTableRoot = vtrs.get(versionIndex.get(0));
            Map<String, TableRule> virtualTableMap = virtualTableRoot.getTableRules();

            // Remove the table rule.
            virtualTableMap.remove(tableName);

            // Remove current table names and regenerate when next call.
            getVersionedTableNames().remove(versionIndex.get(0));
        }
    }

    public TableRule initTableRule(TablesExtRecord record) {
        TableRule tableRule = new TableRule(record.version);

        tableRule.setVirtualTbName(record.tableName);

        tableRule.setDbNamePattern(record.dbNamePattern);

        if (TStringUtil.isNotBlank(record.dbRule)) {
            List<String> dbRules = new ArrayList<>(1);
            String[] dbRuleArray = TStringUtil.split(record.dbRule, RecordConverter.SEMICOLON);
            if (dbRuleArray != null) {
                for (String dbRule : dbRuleArray) {
                    dbRules.add(dbRule);
                }
            }
            tableRule.setDbRuleArray(dbRules);
        }

        tableRule.setTbNamePattern(record.tbNamePattern);

        if (TStringUtil.isNotBlank(record.tbRule)) {
            List<String> tbRules = new ArrayList<>(1);
            String[] tbRuleArray = TStringUtil.split(record.tbRule, RecordConverter.SEMICOLON);
            if (tbRuleArray != null) {
                for (String tbRule : tbRuleArray) {
                    tbRules.add(tbRule);
                }
            }
            tableRule.setTbRuleArray(tbRules);
        }

        if (TStringUtil.isNotBlank(record.dbMetaMap)) {
            Map<String, Object> shardFunctionMetaMap = JSON.parseObject(record.dbMetaMap);
            tableRule.setDbShardFunctionMetaMap(shardFunctionMetaMap);
        }

        if (TStringUtil.isNotBlank(record.tbMetaMap)) {
            Map<String, Object> shardFunctionMetaMap = JSON.parseObject(record.tbMetaMap);
            tableRule.setTbShardFunctionMetaMap(shardFunctionMetaMap);
        }

        if (TStringUtil.isNotBlank(record.extPartitions)) {
            List<MappingRule> extPartitions = convertJSONToExtPartitions(record.extPartitions);
            tableRule.setExtPartitions(extPartitions);
        }

        tableRule.setAllowFullTableScan(record.fullTableScan != 0);
        tableRule.setBroadcast(record.broadcast != 0);

        try {
            tableRule.init();
        } catch (Exception e) {
            LOGGER.error("Failed to initialize table rule for " + record.tableName, e);
        }

        return tableRule;
    }

    private List<MappingRule> convertJSONToExtPartitions(String extPartitionsJSON) {
        if (TStringUtil.isBlank(extPartitionsJSON)) {
            return null;
        }
        List<Map<String, Object>> extPartMaps = (List<Map<String, Object>>) JSON.parse(extPartitionsJSON);
        if (extPartMaps == null || extPartMaps.isEmpty()) {
            return null;
        }
        List<MappingRule> mappingRules = new ArrayList<>();
        for (Map<String, Object> extPartMap : extPartMaps) {
            MappingRule mappingRule = new MappingRule();
            mappingRule.setDbKeyValue((String) extPartMap.get("dbKeyValue"));
            mappingRule.setTbKeyValue((String) extPartMap.get("tbKeyValue"));
            mappingRule.setDb((String) extPartMap.get("db"));
            mappingRule.setTb((String) extPartMap.get("tb"));
            mappingRules.add(mappingRule);
        }
        return mappingRules;
    }

    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

}
