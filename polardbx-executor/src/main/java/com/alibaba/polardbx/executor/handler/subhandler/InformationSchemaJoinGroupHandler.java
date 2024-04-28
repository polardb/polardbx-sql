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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupTableDetailRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaJoinGroup;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class InformationSchemaJoinGroupHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaJoinGroupHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaJoinGroup;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        // only new partitioning db
        Set<String> schemaNames = new TreeSet<>(String::compareToIgnoreCase);
        schemaNames.addAll(JoinGroupUtils.getDistinctSchemaNames(null));

        int schemaIndex = InformationSchemaJoinGroup.getTableSchemaIndex();
        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();
        schemaNames = virtualView.applyFilters(schemaIndex, params, schemaNames);

        List<JoinGroupInfoRecord> joinGroupInfoRecords = JoinGroupUtils.getAllJoinGroupInfos(null);
        for (JoinGroupInfoRecord joinGroupInfoRecord : GeneralUtil.emptyIfNull(joinGroupInfoRecords)) {
            if (!schemaNames.contains(joinGroupInfoRecord.tableSchema)) {
                continue;
            }
            List<JoinGroupTableDetailRecord> joinGroupTableDetailRecords =
                JoinGroupUtils.getJoinGroupDetailByName(joinGroupInfoRecord.tableSchema,
                    joinGroupInfoRecord.joinGroupName,
                    null);
            if (GeneralUtil.isNotEmpty(joinGroupTableDetailRecords)) {
                for (JoinGroupTableDetailRecord joinGroupTableDetailRecord : joinGroupTableDetailRecords) {
                    Object[] row = new Object[5];
                    cursor.addRow(row);
                    row[0] = DataTypes.StringType.convertFrom(joinGroupInfoRecord.tableSchema);
                    row[1] = DataTypes.ULongType.convertFrom(joinGroupInfoRecord.id);
                    row[2] = DataTypes.StringType.convertFrom(joinGroupInfoRecord.joinGroupName);
                    row[3] = DataTypes.StringType.convertFrom(joinGroupInfoRecord.locality);
                    row[4] = DataTypes.StringType.convertFrom(joinGroupTableDetailRecord.tableName);
                }
            } else {
                Object[] row = new Object[5];
                cursor.addRow(row);
                row[0] = DataTypes.StringType.convertFrom(joinGroupInfoRecord.tableSchema);
                row[1] = DataTypes.ULongType.convertFrom(joinGroupInfoRecord.id);
                row[2] = DataTypes.StringType.convertFrom(joinGroupInfoRecord.joinGroupName);
                row[3] = DataTypes.StringType.convertFrom(joinGroupInfoRecord.locality);
                row[4] = null;
            }

        }
        return cursor;
    }
}
