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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.statis.PlanAccessStat;

import java.util.Map;

/**
 * @author chenghui.lch
 */
public class ShowTableJoinClosureSyncAction implements ISyncAction {

    private String db;

    public ShowTableJoinClosureSyncAction() {
    }

    public ShowTableJoinClosureSyncAction(String db) {
        this.db = db;
    }

    @Override
    public ResultCursor sync() {

        ArrayResultCursor result = new ArrayResultCursor("TABLE_JOIN_CLOSURE_STAT");
        result.addColumn("JOIN_CLOSURE_KEY", DataTypes.StringType);
        result.addColumn("JOIN_CLOSURE_TABLE_SET", DataTypes.StringType);
        result.addColumn("JOIN_CLOSURE_SIZE", DataTypes.LongType);
        result.addColumn("ACCESS_COUNT", DataTypes.LongType);
        result.addColumn("TEMPLATE_ID_SET", DataTypes.StringType);
        result.initMeta();

        Map<String, PlanAccessStat.PlanJoinClosureStatInfo> tblJoinClosureStatMap =
            PlanAccessStat.tableJoinClosureStat();
        for (Map.Entry<String, PlanAccessStat.PlanJoinClosureStatInfo> tblAccessItem : tblJoinClosureStatMap.entrySet()) {
            PlanAccessStat.PlanJoinClosureStatInfo statInfo = tblAccessItem.getValue();
            result.addRow(new Object[] {
                statInfo.closureKey,
                statInfo.joinTableSetStr,
                statInfo.joinClosureTableSet.size(),
                statInfo.accessCount,
                statInfo.templateIdSetStr
            });
        }

        return result;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }
}
