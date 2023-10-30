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

package com.alibaba.polardbx.executor.changeset;

import org.apache.commons.lang.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ChangeSetReporter {
    private final ChangeSetMetaManager changeSetMetaManager;

    private ChangeSetMetaManager.ChangeSetBean changeSetBean;

    public ChangeSetReporter(ChangeSetMetaManager changeSetMetaManager) {
        this.changeSetMetaManager = changeSetMetaManager;
    }

    public void loadChangeSetMeta(long changeSetId) {
        changeSetBean = changeSetMetaManager.loadChangeSetMeta(changeSetId);
    }

    public ChangeSetMetaManager.ChangeSetBean getBackfillBean() {
        return changeSetBean;
    }

    public void initChangeSetMeta(long changeSetId, long jobId, long rootJobId,
                                  String tableSchema, String tableName,
                                  String indexSchema, String indexName,
                                  Map<String, Set<String>> sourcePhyTables) {
        List<ChangeSetMetaManager.ChangeSetObjectRecord> records = sourcePhyTables.entrySet()
            .stream()
            .flatMap(e -> e.getValue()
                .stream()
                .map(phyTable -> ChangeSetMetaManager.ChangeSetObjectRecord.create(
                    jobId, changeSetId, rootJobId,
                    tableSchema, tableName, indexSchema, indexName,
                    e.getKey(), phyTable)))
            .collect(Collectors.toList());

        changeSetMetaManager.initChangeSetMeta(records);
    }

    public void updateChangeSetStatus(ChangeSetMetaManager.ChangeSetStatus status) {
        changeSetMetaManager.updateLogicalChangeSetObject(changeSetBean, status);
    }

    public void updatePositionMark(ChangeSetManager.ChangeSetTask task) {
        ChangeSetManager.ChangeSetData data = task.getData();
        ChangeSetManager.ChangeSetMeta meta = data.getMeta();

        final ChangeSetMetaManager.ChangeSetStatus status =
            task.taskStatus == ChangeSetManager.ChangeSetTaskStatus.FINISH ?
                ChangeSetMetaManager.ChangeSetStatus.SUCCESS : ChangeSetMetaManager.ChangeSetStatus.RUNNING;

        ChangeSetMetaManager.ChangeSetObjectRecord record = changeSetBean.getRecord(
            meta.getSourceDbGroupName(),
            meta.getSourcePhysicalTable()
        );

        changeSetMetaManager.updateChangeSetObject(
            record,
            data.fetchTimes,
            data.replayTimes,
            data.deleteRowCount,
            data.replaceRowCount,
            status
        );
    }

    public void updateCatchUpStart(String sourceGroup, String phyTableName) {
        ChangeSetMetaManager.ChangeSetObjectRecord record = changeSetBean.getRecord(
            sourceGroup,
            phyTableName
        );

        if (record.getFetchStartTime() == null) {
            record.setFetchStartTime(
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()));
        }

        changeSetMetaManager.updateChangeSetObject(record, ChangeSetMetaManager.START_CATCHUP);
    }

    public boolean needReCatchUp(String sourceGroup, String phyTableName) {

        ChangeSetMetaManager.ChangeSetObjectRecord record = changeSetBean.getRecord(
            sourceGroup,
            phyTableName
        );

        return StringUtils.equalsIgnoreCase(record.getMessage(), ChangeSetMetaManager.START_CATCHUP);
    }
}
