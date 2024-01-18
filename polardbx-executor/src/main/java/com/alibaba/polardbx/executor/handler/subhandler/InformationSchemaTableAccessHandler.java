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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.statis.PlanAccessStat;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTableAccess;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * @author dylan
 */
public class InformationSchemaTableAccessHandler extends BaseVirtualViewSubClassHandler {

    private static Class showTableAccessSyncActionClass;
    private static Class showTableJoinClosureSyncActionClass;

    static {
        // 只有server支持，这里是暂时改法，后续要将这段逻辑解耦
        try {
            showTableAccessSyncActionClass =
                Class.forName("com.alibaba.polardbx.server.response.ShowTableAccessSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    static {
        // 只有server支持，这里是暂时改法，后续要将这段逻辑解耦
        try {
            showTableJoinClosureSyncActionClass =
                Class.forName("com.alibaba.polardbx.server.response.ShowTableJoinClosureSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public InformationSchemaTableAccessHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaTableAccess;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        ISyncAction showTableAccessAction;
        ISyncAction showTableJoinClosureAction;
        if (showTableAccessSyncActionClass == null) {
            throw new NotSupportException();
        }
        if (showTableJoinClosureSyncActionClass == null) {
            throw new NotSupportException();
        }
        try {
            showTableAccessAction = (ISyncAction) showTableAccessSyncActionClass.getConstructor(String.class)
                .newInstance(executionContext.getSchemaName());
            showTableJoinClosureAction = (ISyncAction) showTableJoinClosureSyncActionClass.getConstructor(String.class)
                .newInstance(executionContext.getSchemaName());
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        List<List<Map<String, Object>>> results =
            SyncManagerHelper.sync(showTableAccessAction, executionContext.getSchemaName());

        List<List<Map<String, Object>>> joinClosureResults =
            SyncManagerHelper.sync(showTableJoinClosureAction, executionContext.getSchemaName());
        List<PlanAccessStat.PlanJoinClosureStatInfo> joinClosureStatInfos =
            PlanAccessStat.collectTableJoinClosureStat(joinClosureResults);

        /**
         * Collect all table access info from all cn nodes
         */
        // key: rel_key,
        // val: statInfo
        Map<String, Map<String, Object>> relKeyToStatInfoMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

        /**
         * The sync result of ShowTableAccessSyncAction of each cn
         */
//        result.addColumn("RELATION_KEY", DataTypes.StringType);
//        result.addColumn("FULL_TABLE_NAME", DataTypes.StringType);
//        result.addColumn("OTHER_TABLE_NAME", DataTypes.StringType);
//        result.addColumn("ACCESS_COUNT", DataTypes.LongType);
//        result.addColumn("TEMPLATE_ID_SET", DataTypes.StringType);
        for (List<Map<String, Object>> rs : results) {
            for (int i = 0; i < rs.size(); i++) {
                Map<String, Object> accessItem = rs.get(i);
                String relKey = (String) accessItem.get("RELATION_KEY");
                Long hitCount = (Long) accessItem.get("ACCESS_COUNT");
                String templateIdSetStr = (String) accessItem.get("TEMPLATE_ID_SET");
                String[] templateIdSetArr = templateIdSetStr.split(",");

                Map<String, Object> oneRelKeyStatInfo = relKeyToStatInfoMap.get(relKey);
                if (oneRelKeyStatInfo == null) {
                    oneRelKeyStatInfo = new HashMap<>();

                    Set<String> templateIdSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                    for (int k = 0; k < templateIdSetArr.length; k++) {
                        templateIdSet.add(templateIdSetArr[k]);
                    }
                    accessItem.put("TEMPLATE_ID_SET_OBJ", templateIdSet);

                    oneRelKeyStatInfo.putAll(accessItem);
                    relKeyToStatInfoMap.put(relKey, oneRelKeyStatInfo);
                } else {
                    Long accessCount = (Long) oneRelKeyStatInfo.get("ACCESS_COUNT");
                    accessCount += hitCount;
                    oneRelKeyStatInfo.put("ACCESS_COUNT", accessCount);

                    Set<String> templateIdSetObj = (Set<String>) oneRelKeyStatInfo.get("TEMPLATE_ID_SET_OBJ");
                    if (templateIdSetObj != null) {
                        for (int j = 0; j < templateIdSetArr.length; j++) {
                            if (!templateIdSetObj.contains(templateIdSetArr[j])) {
                                templateIdSetObj.add(templateIdSetArr[j]);
                            }
                        }
                    }
                }
            }
        }

        for (Map.Entry<String, Map<String, Object>> relKeyToStatInfoItem : relKeyToStatInfoMap.entrySet()) {
            Map<String, Object> statInfo = relKeyToStatInfoItem.getValue();

            String fullTblName = (String) statInfo.get("FULL_TABLE_NAME");
            String joinFullTblName = (String) statInfo.get("OTHER_TABLE_NAME");
            Long accessCount = (Long) statInfo.get("ACCESS_COUNT");
            Set<String> templateIdSetObj = (Set<String>) statInfo.get("TEMPLATE_ID_SET_OBJ");

            if (fullTblName.toLowerCase().startsWith("information_schema.") || fullTblName.toLowerCase()
                .startsWith("__cdc__.")) {
                continue;
            }

            String tblSchema;
            String tblName;
            String[] dbAndTbArr = fullTblName.split("\\.");
            tblSchema = dbAndTbArr[0];
            tblName = dbAndTbArr[1];

            String closureKey = findClosureKey(fullTblName, joinClosureStatInfos);
            Long templateIdCount = Long.valueOf(templateIdSetObj.size());

            String joinTblSchema;
            String joinTblName;
            if (joinFullTblName == null || joinFullTblName.isEmpty()) {
                joinTblName = "";
                joinTblSchema = "";
            } else {
                String[] joinDbAndTbArr = joinFullTblName.split("\\.");
                joinTblSchema = joinDbAndTbArr[0];
                joinTblName = joinDbAndTbArr[1];
            }

            cursor.addRow(new Object[] {
                closureKey,
                tblSchema,
                tblName,
                joinTblSchema,
                joinTblName,
                accessCount,
                templateIdCount
            });
        }

        return cursor;
    }

    private String findClosureKey(String fullTblName, List<PlanAccessStat.PlanJoinClosureStatInfo> closureStatInfos) {
        for (int i = 0; i < closureStatInfos.size(); i++) {
            if (closureStatInfos.get(i).joinClosureTableSet.contains(fullTblName)) {
                return closureStatInfos.get(i).closureKey;
            }
        }
        return null;
    }
}

