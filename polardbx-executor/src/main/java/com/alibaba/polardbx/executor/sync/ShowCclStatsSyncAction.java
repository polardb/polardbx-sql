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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.ccl.CclManager;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;
import com.alibaba.polardbx.optimizer.ccl.service.ICclConfigService;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.List;

/**
 * @author busu
 * date: 2020/10/29 10:25 上午
 */
public class ShowCclStatsSyncAction implements ISyncAction {

    public static final String ID = "Id";
    public static final String RUNNING = "Running";
    public static final String WAITING = "Waiting";
    public static final String KILLED = "Killed";
    public static final String MATCH_HIT_CACHE = "Match_Hit_Cache";
    public static final String TOTAL_MATCH = "Total_Match";

    public ShowCclStatsSyncAction() {

    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("CclStats");
        result.addColumn(ID, DataTypes.StringType);
        result.addColumn(RUNNING, DataTypes.IntegerType);
        result.addColumn(WAITING, DataTypes.IntegerType);
        result.addColumn(KILLED, DataTypes.LongType);
        result.addColumn(MATCH_HIT_CACHE, DataTypes.LongType);
        result.addColumn(TOTAL_MATCH, DataTypes.LongType);
        ICclConfigService cclConfigService = CclManager.getCclConfigService();
        List<CclRuleInfo> cclRuleInfos = cclConfigService.getCclRuleInfos();
        for (CclRuleInfo cclRuleInfo : cclRuleInfos) {
            result.addRow(new Object[] {
                cclRuleInfo.getCclRuleRecord().id, cclRuleInfo.getRunningCount().get(),
                cclRuleInfo.getStayCount().get() - cclRuleInfo.getRunningCount().get(),
                cclRuleInfo.getCclRuntimeStat().killedCount.get(),
                cclRuleInfo.getCclRuntimeStat().matchCclRuleHitCount.get(),
                cclRuleInfo.getCclRuntimeStat().totalMatchCclRuleCount.get()});
        }
        return result;
    }

}
