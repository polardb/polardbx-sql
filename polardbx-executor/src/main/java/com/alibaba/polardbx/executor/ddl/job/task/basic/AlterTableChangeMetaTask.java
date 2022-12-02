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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@TaskName(name = "AlterTableChangeMetaTask")
public class AlterTableChangeMetaTask extends BaseGmsTask {

    private String dbIndex;
    private String phyTableName;
    private SqlKind sqlKind;
    private boolean isPartitioned;

    private List<String> droppedColumns;
    private List<String> addedColumns;
    private List<String> updatedColumns;
    private Map<String, String> changedColumns;

    private boolean hasTimestampColumnDefault;
    private Map<String, String> binaryColumnDefaultValues;

    private List<String> droppedIndexes;
    private List<String> addedIndexes;
    private List<String> addedIndexesWithoutNames;
    private Map<String, String> renamedIndexes;

    private boolean primaryKeyDropped = false;
    private List<String> addedPrimaryKeyColumns;

    private List<Pair<String, String>> columnAfterAnother;
    private boolean requireLogicalColumnOrder;

    private String tableComment;
    private String tableRowFormat;

    private SequenceBean sequenceBean;

    private boolean onlineModifyColumnIndexTask;

    public AlterTableChangeMetaTask(String schemaName,
                                    String logicalTableName,
                                    String dbIndex,
                                    String phyTableName,
                                    SqlKind sqlKind,
                                    boolean isPartitioned,
                                    List<String> droppedColumns,
                                    List<String> addedColumns,
                                    List<String> updatedColumns,
                                    Map<String, String> changedColumns,
                                    boolean hasTimestampColumnDefault,
                                    Map<String, String> binaryColumnDefaultValues,
                                    List<String> droppedIndexes,
                                    List<String> addedIndexes,
                                    List<String> addedIndexesWithoutNames,
                                    Map<String, String> renamedIndexes,
                                    boolean primaryKeyDropped,
                                    List<String> addedPrimaryKeyColumns,
                                    List<Pair<String, String>> columnAfterAnother,
                                    boolean requireLogicalColumnOrder,
                                    String tableComment,
                                    String tableRowFormat,
                                    SequenceBean sequenceBean,
                                    boolean onlineModifyColumnIndexTask) {
        super(schemaName, logicalTableName);
        this.dbIndex = dbIndex;
        this.phyTableName = phyTableName;
        this.sqlKind = sqlKind;
        this.isPartitioned = isPartitioned;
        this.droppedColumns = droppedColumns;
        this.addedColumns = addedColumns;
        this.updatedColumns = updatedColumns;
        this.changedColumns = changedColumns;
        this.hasTimestampColumnDefault = hasTimestampColumnDefault;
        this.binaryColumnDefaultValues = binaryColumnDefaultValues;
        this.droppedIndexes = droppedIndexes;
        this.addedIndexes = addedIndexes;
        this.addedIndexesWithoutNames = addedIndexesWithoutNames;
        this.renamedIndexes = renamedIndexes;
        this.primaryKeyDropped = primaryKeyDropped;
        this.addedPrimaryKeyColumns = addedPrimaryKeyColumns;
        this.columnAfterAnother = columnAfterAnother;
        this.requireLogicalColumnOrder = requireLogicalColumnOrder;
        this.tableComment = tableComment;
        this.tableRowFormat = tableRowFormat;
        this.sequenceBean = sequenceBean;
        this.onlineModifyColumnIndexTask = onlineModifyColumnIndexTask;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        TableMetaChanger.changeTableMeta(metaDbConnection, schemaName, logicalTableName, dbIndex, phyTableName, sqlKind,
            isPartitioned, droppedColumns, addedColumns, updatedColumns, changedColumns, hasTimestampColumnDefault,
            binaryColumnDefaultValues, droppedIndexes, addedIndexes, addedIndexesWithoutNames, renamedIndexes,
            primaryKeyDropped, addedPrimaryKeyColumns, columnAfterAnother, requireLogicalColumnOrder, tableComment,
            tableRowFormat, sequenceBean, onlineModifyColumnIndexTask, executionContext);

        List<String> columnStatsRemoved = new ArrayList<>();
        if (GeneralUtil.isNotEmpty(droppedColumns)) {
            columnStatsRemoved.addAll(droppedColumns);
        }
        if (GeneralUtil.isNotEmpty(updatedColumns)) {
            columnStatsRemoved.addAll(updatedColumns);
        }
        if (GeneralUtil.isNotEmpty(changedColumns)) {
            changedColumns.keySet().stream().forEach(c -> columnStatsRemoved.add(c));
        }
        CommonMetaChanger.finalOperationsOnAlterTableSuccess(schemaName, logicalTableName, columnStatsRemoved);
    }

    @Override
    public String remark() {
        StringBuilder sb = new StringBuilder();
        if (CollectionUtils.isNotEmpty(this.addedColumns)) {
            sb.append("add columns ").append(this.addedColumns);
        }
        if (CollectionUtils.isNotEmpty(this.droppedColumns)) {
            sb.append("drop columns ").append(this.droppedColumns);
        }
        if (CollectionUtils.isNotEmpty(this.addedIndexes)) {
            sb.append("add indexes ").append(this.addedIndexes);
        }
        if (CollectionUtils.isNotEmpty(this.addedIndexesWithoutNames)) {
            sb.append("add indexes without names").append(this.addedIndexesWithoutNames);
        }
        if (CollectionUtils.isNotEmpty(this.droppedIndexes)) {
            sb.append("drop indexes ").append(this.droppedIndexes);
        }
        sb.append(" on table ").append(this.getLogicalTableName());
        return "|" + sb;
    }

}
