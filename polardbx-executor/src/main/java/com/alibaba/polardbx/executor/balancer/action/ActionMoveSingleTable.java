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

package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

/**
 * Action that move group between storage nodes
 *
 * @author moyi
 * @since 2021/05
 */
public class ActionMoveSingleTable implements BalanceAction {

    private static final Logger LOG = SQLRecorderLogger.ddlLogger;

    private String schema;
    private int newTableGroupNum;

    /**
     * Run move database in debug mode.
     */
    private boolean debug = false;
    private BalanceStats stats;

    public ActionMoveSingleTable(String schema, int newTableGroupNum) {
        this.schema = schema;
        this.newTableGroupNum = newTableGroupNum;
    }

    @Override
    public String getSchema() {
        return this.schema;
    }

    @Override
    public String getName() {
        return "BuildNewSingleTableGroup";
    }

    @Override
    public String getStep() {
        return genBuildTableGroupSql(newTableGroupNum);
    }

    private String genBuildTableGroupSql(int n) {
        String sql = "";
        for (int i = 0; i < n; i++) {
            sql += "create tablegroup single_tg";
        }
        return sql;
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
//        String groups = StringUtils.join(getSourceGroups(), ",");
//        String sql = genMoveGroupSql(groups);
//        if (this.debug) {
//            ParamManager.setBooleanVal(ec.getParamManager().getProps(),
//                ConnectionParams.SHARE_STORAGE_MODE, true, false);
//            ParamManager.setVal(ec.getParamManager().getProps(),
//                ConnectionParams.SCALE_OUT_DEBUG, "true", false);
//        }
//        long totalRows = 0L;
//        long totalSize = 0L;
//        try {
//            if (!DbInfoManager.getInstance().isNewPartitionDb(schema)) {
//                for (GroupStats.GroupsOfStorage groupsOfStorage : GeneralUtil.emptyIfNull(stats.getGroups())) {
//                    if (groupsOfStorage == null || groupsOfStorage.getGroupDataSizeMap() == null) {
//                        continue;
//                    }
//                    for (Map.Entry<String, Pair<Long, Long>> entry : groupsOfStorage.groupDataSizeMap.entrySet()) {
//                        if (sourceGroups.contains(entry.getKey())) {
//                            totalRows += entry.getValue().getKey();
//                            totalSize += entry.getValue().getValue();
//                        }
//                    }
//                }
//            }
//        } catch (Exception e) {
//            EventLogger.log(EventType.DDL_WARN, "calculate rebalance rows error. " + e.getMessage());
//        }
//
//        return ActionUtils.convertToDelegatorJob(schema, sql,
//            CostEstimableDdlTask.createCostInfo(totalRows, totalSize));
        return new ExecutableDdlJob();
    }

    @Override
    public String toString() {
        return "ActionBuildNewBalanceSingleTableGroup{" +
            ", debug=" + debug +
            '}';
    }

}
