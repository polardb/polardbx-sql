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

package com.alibaba.polardbx.executor.ddl.job.meta;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.sql.SqlAlterRule;

import java.sql.Connection;
import java.sql.SQLException;

public class AlterRuleMetaChanger {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlterRuleMetaChanger.class);

    public static void alterRule(String schemaName, SqlAlterRule sqlAlterRule) {
        String tableName = sqlAlterRule.getName().toString();

        TableRule tableRule = OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(tableName);
        if (tableRule == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_PASS_RULE_VALIDATE,
                "the table rule for '" + tableName + "' doesn't exist");
        }

        String tableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, tableName);

        TableInfoManager tableInfoManager = new TableInfoManager();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            try {
                MetaDbUtil.beginTransaction(metaDbConn);

                TablesExtRecord existingRecord = tableInfoManager.queryTableExt(schemaName, tableName, false);

                if (sqlAlterRule.getAllowFullTableScan() != -1) {
                    existingRecord.fullTableScan = sqlAlterRule.getAllowFullTableScan();
                }

                if (sqlAlterRule.getBroadcast() != -1) {
                    existingRecord.broadcast = sqlAlterRule.getBroadcast();
                }

                tableInfoManager.updateTableExt(existingRecord);

                TableInfoManager.updateTableVersion(schemaName, tableName, metaDbConn);

                MetaDbUtil.commit(metaDbConn);
            } catch (SQLException e) {
                MetaDbUtil.rollback(metaDbConn, e, LOGGER, schemaName, tableName, "alter rule properties");
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
            }
            CommonMetaChanger.sync(tableDataId);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }
    }

}
