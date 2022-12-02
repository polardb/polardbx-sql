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

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.backfill.Throttle;
import com.alibaba.polardbx.executor.backfill.ThrottleInfo;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlPlanManager;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlBackFillSpeedSyncAction;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.scheduler.DdlPlanRecord;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaRebalanceBackFill;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author guxu
 */
public class InformationSchemaRebalanceBackFillHandler extends BaseVirtualViewSubClassHandler {

    static final Logger LOGGER = LoggerFactory.getLogger(InformationSchemaRebalanceBackFillHandler.class);

    public InformationSchemaRebalanceBackFillHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }


    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaRebalanceBackFill;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        return buildRebalanceBackFillView(cursor);
    }

    public static Cursor buildRebalanceBackFillView(ArrayResultCursor cursor){
        GsiBackfillManager backfillManager = new GsiBackfillManager(SystemDbHelper.DEFAULT_DB_NAME);
        DdlJobManager ddlJobManager = new DdlJobManager();
        List<DdlEngineRecord> ddlRecordList = ddlJobManager.fetchRecords(DdlState.ALL_STATES);
        //filter out REBALANCE JOBs
        ddlRecordList = ddlRecordList.stream().filter(e -> DdlType.valueOf(e.ddlType) == DdlType.REBALANCE).collect(Collectors.toList());
        if(CollectionUtils.isEmpty(ddlRecordList)){
            return cursor;
        }

        Map<Long, ThrottleInfo> throttleInfoMap = collectThrottleInfoMap();

        for (DdlEngineRecord record : ddlRecordList) {
            final Long jobId = record.jobId;

            List<DdlEngineTaskRecord> allTasks = ddlJobManager.fetchAllSuccessiveTaskByJobId(jobId);
            List<DdlEngineTaskRecord> allRootTasks =
                    allTasks.stream().filter(e -> e.getJobId() == jobId).collect(Collectors.toList());
            //all submitted BackFill tasks, and yet there may be some BackFill tasks haven't been submitted
            List<DdlEngineTaskRecord> allBackFillTasks =
                    allTasks.stream().filter(e -> StringUtils.containsIgnoreCase(e.getName(), "BackFill"))
                            .collect(Collectors.toList());
            List<GsiBackfillManager.BackFillAggInfo> backFillAggInfoList =
                    backfillManager.queryBackFillAggInfoById(allBackFillTasks.stream().map(e -> e.taskId).collect(Collectors.toList()));

            long totalRows = 0L;
            long totalSize = 0L;
            for (DdlEngineTaskRecord taskRecord : allRootTasks) {
                if (StringUtils.isEmpty(taskRecord.getCost())) {
                    continue;
                }
                CostEstimableDdlTask.CostInfo costInfo = TaskHelper.decodeCostInfo(taskRecord.getCost());
                totalRows += costInfo.rows;
                totalSize += costInfo.dataSize;
            }

            for (GsiBackfillManager.BackFillAggInfo backFillAggInfo : backFillAggInfoList) {
                ThrottleInfo throttleInfo = throttleInfoMap.get(backFillAggInfo.getBackFillId());
                long duration = backFillAggInfo.getDuration()==0 ? 1L: backFillAggInfo.getDuration();
                addRow(
                        cursor,
                        jobId,
                        backFillAggInfo.getBackFillId(),
                        backFillAggInfo.getTableSchema(),
                        backFillAggInfo.getStartTime(),
                        GsiBackfillManager.BackfillStatus.display(backFillAggInfo.getStatus()),
                        throttleInfo==null? "0" : throttleInfo.getSpeed(),
                        backFillAggInfo.getSuccessRowCount()/duration,
                        backFillAggInfo.getSuccessRowCount(),
                        totalRows
                );
            }
        }

        return cursor;
    }

    private static Map<Long, ThrottleInfo> collectThrottleInfoMap(){
        Map<Long, ThrottleInfo> throttleInfoMap = new HashMap<>();
        for (ThrottleInfo throttleInfo : Throttle.getThrottleInfoList()) {
            throttleInfoMap.put(throttleInfo.getBackFillId(), throttleInfo);
        }
        try {
            List<List<Map<String, Object>>> result = SyncManagerHelper.sync(
                    new DdlBackFillSpeedSyncAction(), SystemDbHelper.DEFAULT_DB_NAME, SyncScope.MASTER_ONLY);
            for (List<Map<String, Object>> list : GeneralUtil.emptyIfNull(result)) {
                for (Map<String, Object> map : GeneralUtil.emptyIfNull(list)) {
                    throttleInfoMap.put(Long.parseLong(String.valueOf(map.get("BACKFILL_ID"))),
                            new ThrottleInfo(
                                    Long.parseLong(String.valueOf(map.get("BACKFILL_ID"))),
                                    Double.parseDouble(String.valueOf(map.get("SPEED"))),
                                    Long.parseLong(String.valueOf(map.get("TOTAL_ROWS")))
                            ));
                }
            }
        }catch (Exception e){
            LOGGER.error("collect ThrottleInfo from remote nodes error", e);
        }
        return throttleInfoMap;
    }

    private static void addRow(ArrayResultCursor cursor, long jobId, long taskId, String schemaName,
                        String startTime,
                        String state, Object currentSpeed, Object averageSpeed, long finishedRows, long totalRows) {
        cursor.addRow(new Object[]{jobId, taskId, schemaName, startTime, state, currentSpeed, averageSpeed, finishedRows, totalRows});
    }

}

