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

package com.alibaba.polardbx.executor.ddl.newengine.sync;

import com.alibaba.polardbx.executor.backfill.Throttle;
import com.alibaba.polardbx.executor.backfill.ThrottleInfo;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import lombok.Data;

@Data
public class DdlBackFillSpeedSyncAction implements ISyncAction {

    private Long backFillId;
    private double speed;
    private long totalRows;

    public DdlBackFillSpeedSyncAction() {
    }

    public DdlBackFillSpeedSyncAction(Long backFillId, double speed, long totalRows) {
        this.backFillId = backFillId;
        this.speed = speed;
        this.totalRows = totalRows;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor resultCursor = buildResultCursor();
        for(ThrottleInfo throttleInfo: Throttle.getThrottleInfoList()){
            resultCursor.addRow(buildRow(throttleInfo));
        }
        return resultCursor;
    }

    public static ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("BACKFILL_INFO");
        resultCursor.addColumn("BACKFILL_ID", DataTypes.StringType);
        resultCursor.addColumn("SPEED", DataTypes.StringType);
        resultCursor.addColumn("TOTAL_ROWS", DataTypes.StringType);
        resultCursor.initMeta();
        return resultCursor;
    }


    private Object[] buildRow(ThrottleInfo throttleInfo) {
        return new Object[] {
                throttleInfo.getBackFillId(),
                throttleInfo.getSpeed(),
                throttleInfo.getTotalRows()
        };
    }


}
