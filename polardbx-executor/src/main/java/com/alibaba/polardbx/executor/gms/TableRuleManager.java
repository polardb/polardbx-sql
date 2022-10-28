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

package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TableRuleManager {

    public static void reload(String schemaName, String tableName) {
        TddlRule tddlRule = OptimizerContext.getContext(schemaName).getRuleManager().getTddlRule();
        tddlRule.addRule(tableName, true);
    }

    public static void invalidate(String schemaName, String tableName) {
        TddlRule tddlRule = OptimizerContext.getContext(schemaName).getRuleManager().getTddlRule();
        tddlRule.removeRule(tableName);
    }

    public static Map<String, TableRule> getTableRules(String schemaName) {
        Map<String, TableRule> tableRuleSet = new HashMap<>();
        Collection<TableRule> tableRules =
            OptimizerContext.getContext(schemaName).getRuleManager().getTddlRule().getTables();
        if (tableRules != null && tableRules.size() > 0) {
            for (TableRule tableRule : tableRules) {
                tableRuleSet.put(tableRule.getVirtualTbName().toLowerCase(), tableRule);
            }
        }
        return tableRuleSet;
    }

    public static TableRule getTableRule(String schemaName, String tableName) {
        TddlRule tddlRule = OptimizerContext.getContext(schemaName).getRuleManager().getTddlRule();
        TableRule tableRule = tddlRule.getTable(tableName);
        if (tableRule == null) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                tableInfoManager.setConnection(metaDbConn);
                // Get table meta even if it's invisible.
                TablesExtRecord record = tableInfoManager.queryTableExt(schemaName, tableName, false);
                if (record != null) {
                    tableRule = tddlRule.initTableRule(record);
                }
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
            } finally {
                tableInfoManager.setConnection(null);
            }
        }
        return tableRule;
    }

}
