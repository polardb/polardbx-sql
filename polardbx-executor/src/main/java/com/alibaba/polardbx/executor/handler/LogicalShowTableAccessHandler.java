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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.statis.PlanAccessStat;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author chenmo.cm
 */
public class LogicalShowTableAccessHandler extends HandlerCommon {

    private static Class showTableAccessSyncActionClass;

    static {
        // 只有server支持，这里是暂时改法，后续要将这段逻辑解耦
        try {
            showTableAccessSyncActionClass =
                Class.forName("com.alibaba.polardbx.server.response.ShowTableAccessSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public LogicalShowTableAccessHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        //final LogicalShow show = (LogicalShow) logicalPlan;
        //final SqlShowTableAccess showTableAccess = (SqlShowTableAccess) show.getNativeSqlNode();

        ISyncAction showTableAccessAction;
        if (showTableAccessSyncActionClass == null) {
            throw new NotSupportException();
        }
        try {
            showTableAccessAction = (ISyncAction) showTableAccessSyncActionClass.getConstructor(String.class)
                .newInstance(executionContext.getSchemaName());
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        List<List<Map<String, Object>>> tableAccessStatOfAllCn =
            SyncManagerHelper.sync(showTableAccessAction, executionContext.getSchemaName());

        ArrayResultCursor cursor = new ArrayResultCursor("SHOW_TABLE_ACCESS");
        cursor.addColumn("TABLE_SCHEMA", DataTypes.StringType);
        cursor.addColumn("TABLE_NAME", DataTypes.StringType);
        cursor.addColumn("TABLE_TYPE", DataTypes.StringType);
        cursor.addColumn("ACCESS_COUNT", DataTypes.LongType);
        cursor.initMeta();

        /**
         * Collect all table access info from all cn nodes
         */
        Map<String, Map<String, Object>> fullTblToStatInfoMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (List<Map<String, Object>> tableAccessStatInfoOfOneCn : tableAccessStatOfAllCn) {
            for (int i = 0; i < tableAccessStatInfoOfOneCn.size(); i++) {
                Map<String, Object> statInfo = tableAccessStatInfoOfOneCn.get(i);
                String fullTable = (String) statInfo.get("FULL_TABLE_NAME");
                String otherTable = (String) statInfo.get("OTHER_TABLE_NAME");
                Long accessCount = (Long) statInfo.get("ACCESS_COUNT");
                if (otherTable == null || otherTable.isEmpty()) {
                    Map<String, Object> statInfoOfOneTbl = fullTblToStatInfoMap.get(fullTable);
                    if (statInfoOfOneTbl == null) {
                        fullTblToStatInfoMap.put(fullTable, statInfo);
                    } else {
                        Long allCount = (Long) statInfoOfOneTbl.get("ACCESS_COUNT");
                        allCount += accessCount;
                        statInfoOfOneTbl.put("ACCESS_COUNT", allCount);
                    }
                }
            }
        }

        for (Map.Entry<String, Map<String, Object>> fullTblStatItem : fullTblToStatInfoMap.entrySet()) {
            Map<String, Object> statInfo = fullTblStatItem.getValue();
            String fullTable = (String) statInfo.get("FULL_TABLE_NAME");
            Long accessCount = (Long) statInfo.get("ACCESS_COUNT");

            String[] dbAndTbOfFullTbl = fullTable.split("\\.");
            String db = dbAndTbOfFullTbl[0];
            String tb = dbAndTbOfFullTbl[1];
            String tbType = PlanAccessStat.fetchTableType(db, tb);
            cursor.addRow(new Object[] {
                db,
                tb,
                tbType,
                accessCount
            });
        }

        return cursor;
    }
}
