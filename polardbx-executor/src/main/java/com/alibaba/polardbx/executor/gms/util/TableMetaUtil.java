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

package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.metadb.record.RecordConverter;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.TableType;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.RuleCompatibleHelper;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.meta.ShardFunctionMeta;
import com.alibaba.polardbx.rule.utils.ShardFunctionMetaUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableMetaUtil {

    private static final String EMPTY_CONTENT = RecordConverter.EMPTY_CONTENT;

    public static TablesExtRecord convertToTablesExtRecord(TableRule newTableRule, String tableSchema, String tableName,
                                                           boolean isGsi, boolean isAutoPartition) {
        TablesExtRecord record = new TablesExtRecord();
        record.tableSchema = tableSchema;
        record.tableName = tableName;
        record.newTableName = EMPTY_CONTENT;
        record.dbPartitionKey = concat(newTableRule.getDbPartitionKeys());
        record.dbPartitionPolicy = getDbPartitionPolicy(newTableRule);
        record.dbPartitionCount = newTableRule.getActualDbCount();
        record.dbNamePattern = newTableRule.getDbNamePattern();
        record.dbRule = concat(newTableRule.getDbRuleStrs());
        record.dbMetaMap = convertShardFunctionMetaToJSON(newTableRule.getDbShardFunctionMeta());
        record.tbPartitionKey = concat(newTableRule.getTbPartitionKeys());
        record.tbPartitionPolicy = getTbPartitionPolicy(newTableRule);
        record.tbPartitionCount = newTableRule.getActualTopology().values().iterator().next().size();
        record.tbNamePattern = newTableRule.getTbNamePattern();
        record.tbRule = concat(newTableRule.getTbRulesStrs());
        record.tbMetaMap = convertShardFunctionMetaToJSON(newTableRule.getTbShardFunctionMeta());
        record.extPartitions = convertExtPartitionsToJSON(newTableRule.getExtPartitions());
        record.fullTableScan = newTableRule.isAllowFullTableScan() ? 1 : 0;
        record.broadcast = newTableRule.isBroadcast() ? 1 : 0;
        record.version = 1;
        record.status = TableStatus.ABSENT.getValue();
        record.flag = isAutoPartition ? TablesExtRecord.FLAG_AUTO_PARTITION : 0;
        setTableType(isGsi, record);
        return record;
    }

    private static void setTableType(boolean isGsi, TablesExtRecord record) {
        boolean isShardingDb = TStringUtil.isNotBlank(record.dbPartitionKey) && record.dbPartitionCount > 1;
        boolean isShardingTb = TStringUtil.isNotBlank(record.tbPartitionKey) && record.tbPartitionCount > 1;
        if (isGsi) {
            record.tableType = TableType.GSI.getValue();
        } else if (isShardingDb || isShardingTb) {
            record.tableType = TableType.SHARDING.getValue();
        } else if (record.broadcast == 1) {
            record.tableType = TableType.BROADCAST.getValue();
        } else {
            record.tableType = TableType.SINGLE.getValue();
        }
    }

    private static String getDbPartitionPolicy(TableRule tableRule) {
        String dbPartitionPolicy = EMPTY_CONTENT;
        if (tableRule.getDbShardFunctionMeta() != null) {
            dbPartitionPolicy = tableRule.getDbShardFunctionMeta().buildCreateTablePartitionFunctionStr();
        } else if (tableRule.getDbRuleStrs() != null && tableRule.getDbRuleStrs().length > 0) {
            String dbRule = tableRule.getDbRuleStrs()[0];
            dbPartitionPolicy = getPartitionPolicy(dbRule);
        }
        return dbPartitionPolicy;
    }

    private static String getTbPartitionPolicy(TableRule tableRule) {
        String tbPartitionPolicy = EMPTY_CONTENT;
        if (tableRule.getTbShardFunctionMeta() != null) {
            tbPartitionPolicy = tableRule.getTbShardFunctionMeta().buildCreateTablePartitionFunctionStr();
        } else if (tableRule.getTbRulesStrs() != null && tableRule.getTbRulesStrs().length > 0) {
            String tbRule = tableRule.getTbRulesStrs()[0];
            tbPartitionPolicy = getPartitionPolicy(tbRule);
        }
        return tbPartitionPolicy;
    }

    private static String getPartitionPolicy(String rule) {
        String partitionPolicy = null;

        if (TStringUtil.containsIgnoreCase(rule, "yyyymm_i_opt")) {
            partitionPolicy = "YYYYMM_OPT";
        } else if (TStringUtil.containsIgnoreCase(rule, "yyyydd_i_opt")) {
            partitionPolicy = "YYYYDD_OPT";
        } else if (TStringUtil.containsIgnoreCase(rule, "yyyyweek_i_opt")) {
            partitionPolicy = "YYYYWEEK_OPT";
        } else if (TStringUtil.containsIgnoreCase(rule, "yyyymm")) {
            partitionPolicy = "YYYYMM";
        } else if (TStringUtil.containsIgnoreCase(rule, "yyyydd")) {
            partitionPolicy = "YYYYDD";
        } else if (TStringUtil.containsIgnoreCase(rule, "yyyyweek")) {
            partitionPolicy = "YYYYWEEK";
        } else if (TStringUtil.containsIgnoreCase(rule, "mmdd")) {
            partitionPolicy = "MMDD";
        } else if (TStringUtil.containsIgnoreCase(rule, "mm")) {
            partitionPolicy = "MM";
        } else if (TStringUtil.containsIgnoreCase(rule, "dd")) {
            partitionPolicy = "DD";
        } else if (TStringUtil.containsIgnoreCase(rule, "week")) {
            partitionPolicy = "WEEK";
        } else if (TStringUtil.containsIgnoreCase(rule, "hashCode")) {
            partitionPolicy = "HASH";
        } else if (TStringUtil.containsIgnoreCase(rule, "longValue")) {
            partitionPolicy = "HASH";
        }

        return partitionPolicy;
    }

    private static String convertShardFunctionMetaToJSON(ShardFunctionMeta shardFunctionMeta) {
        if (shardFunctionMeta == null) {
            return EMPTY_CONTENT;
        }
        Map<String, Object> shardFuncMap = ShardFunctionMetaUtils.convertShardFuncMetaToPropertyMap(shardFunctionMeta);
        shardFuncMap.put(ShardFunctionMetaUtils.CLASS_URL,
            RuleCompatibleHelper.forwardCompatibleShardFunctionMeta(shardFunctionMeta.getClass().getName()));
        return JSON.toJSONString(shardFuncMap);
    }

    public static String convertExtPartitionsToJSON(List<MappingRule> mappingRules) {
        if (mappingRules == null || mappingRules.isEmpty()) {
            return EMPTY_CONTENT;
        }
        List<Map<String, Object>> extPartMaps = new ArrayList<>(mappingRules.size());
        for (MappingRule mappingRule : mappingRules) {
            Map<String, Object> extPartMap = new HashMap<>(4);
            extPartMap.put("dbKeyValue", mappingRule.getDbKeyValue());
            extPartMap.put("tbKeyValue", mappingRule.getTbKeyValue());
            extPartMap.put("db", mappingRule.getDb());
            extPartMap.put("tb", mappingRule.getTb());
            extPartMaps.add(extPartMap);
        }
        return JSON.toJSONString(extPartMaps);
    }

    private static String concat(List<String> infoList) {
        if (infoList == null || infoList.isEmpty()) {
            return EMPTY_CONTENT;
        }
        return concat(Arrays.copyOf(infoList.toArray(), infoList.size(), String[].class));
    }

    private static String concat(String[] infoArray) {
        if (infoArray == null || infoArray.length == 0) {
            return EMPTY_CONTENT;
        }
        StringBuilder sb = new StringBuilder();
        for (String info : infoArray) {
            sb.append(RecordConverter.SEMICOLON).append(info);
        }
        return sb.deleteCharAt(0).toString();
    }

}
