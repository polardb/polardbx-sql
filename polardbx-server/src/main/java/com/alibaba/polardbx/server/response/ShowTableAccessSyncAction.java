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
public class ShowTableAccessSyncAction implements ISyncAction {

    private String db;

    public ShowTableAccessSyncAction() {
    }

    public ShowTableAccessSyncAction(String db) {
        this.db = db;
    }

    @Override
    public ResultCursor sync() {

        ArrayResultCursor result = new ArrayResultCursor("TABLE_ACCESS_STAT");
        result.addColumn("RELATION_KEY", DataTypes.StringType);
        result.addColumn("FULL_TABLE_NAME", DataTypes.StringType);
        result.addColumn("OTHER_TABLE_NAME", DataTypes.StringType);
        result.addColumn("ACCESS_COUNT", DataTypes.LongType);
        result.addColumn("TEMPLATE_ID_SET", DataTypes.StringType);
        result.initMeta();

        Map<String, PlanAccessStat.TableJoinStatInfo> tblAccessStat = PlanAccessStat.statTableAccess();
        for (Map.Entry<String, PlanAccessStat.TableJoinStatInfo> tblAccessItem : tblAccessStat.entrySet()) {
            PlanAccessStat.TableJoinStatInfo statInfo = tblAccessItem.getValue();
            result.addRow(new Object[] {
                statInfo.relationKey,
                statInfo.tblName,
                statInfo.otherTblName,
                statInfo.accessCount,
                statInfo.templateIdSetStr
            });
        }

        Map<String, PlanAccessStat.TableJoinStatInfo> tblJoinAccessStat = PlanAccessStat.statTableJoinAccessStat();
        for (Map.Entry<String, PlanAccessStat.TableJoinStatInfo> tblJoinAccessStatItem : tblJoinAccessStat.entrySet()) {
            String relKey = tblJoinAccessStatItem.getKey();
            PlanAccessStat.TableJoinStatInfo statInfo = tblJoinAccessStatItem.getValue();
            result.addRow(new Object[] {
                relKey,
                statInfo.tblName,
                statInfo.otherTblName,
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
