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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.InspectRuleVersionSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.rule.database.util.TddlRuleParam.CONSISTENCY_CONTENT_DIFF;
import static com.alibaba.polardbx.rule.database.util.TddlRuleParam.CONSISTENCY_LEADER;
import static com.alibaba.polardbx.rule.database.util.TddlRuleParam.CONSISTENCY_READ_ONLY;
import static com.alibaba.polardbx.rule.database.util.TddlRuleParam.CONSISTENCY_SOURCE;
import static com.alibaba.polardbx.rule.database.util.TddlRuleParam.CONSISTENCY_TABLE_COUNT;

public class InspectRuleVersionHandler extends AbstractDalHandler {

    private static final String NOT_LEADER = "";

    public InspectRuleVersionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    Cursor doHandle(LogicalDal logicalPlan, ExecutionContext executionContext) {

        ArrayResultCursor resultCursor = new ArrayResultCursor("RULE_INFO");

        handleGMS(resultCursor, executionContext);

        return resultCursor;
    }

    private void handleGMS(ArrayResultCursor resultCursor, ExecutionContext executionContext) {
        resultCursor.addColumn(CONSISTENCY_SOURCE, DataTypes.StringType);
        resultCursor.addColumn(CONSISTENCY_TABLE_COUNT, DataTypes.StringType);
        resultCursor.addColumn(CONSISTENCY_CONTENT_DIFF, DataTypes.StringType);
        resultCursor.addColumn(CONSISTENCY_LEADER, DataTypes.StringType);
        resultCursor.addColumn(CONSISTENCY_READ_ONLY, DataTypes.StringType);
        resultCursor.initMeta();

        Pair<Integer, Integer> tableCount = fetchTableCount(executionContext.getSchemaName());
        resultCursor.addRow(new Object[] {"META_DB", tableCount.getKey() + "/" + tableCount.getValue(), "", "", ""});

        List<List<Map<String, Object>>> resultSets = SyncManagerHelper.sync(new InspectRuleVersionSyncAction(),
            SyncScope.CURRENT_ONLY);
        for (List<Map<String, Object>> resultSet : resultSets) {
            if (resultSet != null && resultSet.size() > 0) {
                for (Map<String, Object> row : resultSet) {
                    resultCursor.addRow(new Object[] {
                        row.get(CONSISTENCY_SOURCE),
                        row.get(CONSISTENCY_TABLE_COUNT),
                        row.get(CONSISTENCY_CONTENT_DIFF),
                        row.get(CONSISTENCY_LEADER),
                        row.get(CONSISTENCY_READ_ONLY)
                    });
                }
            }
        }
    }

    private Pair<Integer, Integer> fetchTableCount(String schemaName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            List<TablesExtRecord> records = tableInfoManager.queryTableExts(schemaName);
            int countVisible = 0, countInvisible = 0;
            if (records != null && records.size() > 0) {
                for (TablesExtRecord record : records) {
                    if (record.status == TableStatus.PUBLIC.getValue()) {
                        countVisible++;
                    } else if (record.status == TableStatus.ABSENT.getValue()) {
                        countInvisible++;
                    }
                }
            }
            return new Pair<>(countVisible, countInvisible);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }
    }
}
