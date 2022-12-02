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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineAccessor;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class ComplexTaskMetaManager extends AbstractLifecycle {

    /**
     * key: id
     * value: ComplexTaskOutlineRecord
     */
    final protected Map<Long, ComplexTaskOutlineRecord> complexTaskOutlineCache;

    public static final int INITED_STATUS = -2;

    private final String schemaName;
    // key:tableGroupName#objectName
    private volatile Map<String, ParentComplexTaskStatusInfo> complexTaskStatusInfoMap;
    private ComplexTaskMetaListener complexTaskMetaListener;

    public ComplexTaskMetaManager(String schemaName) {
        this.schemaName = schemaName;
        this.complexTaskOutlineCache = new HashMap<>();
        this.complexTaskStatusInfoMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.complexTaskMetaListener = new ComplexTaskMetaListener(schemaName);
    }

    public void reload() {
        ComplexTaskOutlineAccessor accessor = new ComplexTaskOutlineAccessor();
        try (Connection connection = MetaDbUtil.getConnection()) {
            accessor.setConnection(connection);
            List<ComplexTaskOutlineRecord> complexTaskOutlineRecords = accessor.getAllUnFinishComplexTask();
            synchronized (complexTaskOutlineCache) {
                complexTaskOutlineCache.clear();
                for (ComplexTaskOutlineRecord record : complexTaskOutlineRecords) {
                    complexTaskOutlineCache.put(record.getId(), record);
                }
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    @Override
    protected void doInit() {
        reload();
        MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getDbComplexTaskDataId(schemaName), null);
        MetaDbConfigManager.getInstance()
            .bindListener(MetaDbDataIdBuilder.getDbComplexTaskDataId(schemaName), complexTaskMetaListener);
    }

    public enum ComplexTaskStatus {

        /**
         * ==================================
         * The following status for one logical table level specially
         */
        /**
         * create phy table
         */
        INITED(INITED_STATUS),
        CREATING(0),
        DELETE_ONLY(1),
        WRITE_ONLY(2),

        /**
         * backfill data
         */
        WRITE_REORG(3),
        READY_TO_PUBLIC(4),
        ABSENT(7),

        /**
         * ==================================
         * The following status for one table group level specially
         */
        DOING_REORG(10),
        SOURCE_WRITE_ONLY(11),
        SOURCE_DELETE_ONLY(12),
        SOURCE_ABSENT(13),
        /**
         * ==================================
         * for scaleout
         */
        FINISH_DB_MIG(14),
        DB_READONLY(15),
        PUBLIC(16);

        private final int value;

        ComplexTaskStatus(int value) {
            this.value = value;
        }

        public static ComplexTaskStatus from(int value) {
            switch (value) {
            case INITED_STATUS:
                return INITED;
            case 0:
                return CREATING;
            case 1:
                return DELETE_ONLY;
            case 2:
                return WRITE_ONLY;
            case 3:
                return WRITE_REORG;
            case 4:
                return READY_TO_PUBLIC;
            case 7:
                return ABSENT;
            case 10:
                return DOING_REORG;
            case 11:
                return SOURCE_WRITE_ONLY;
            case 12:
                return SOURCE_DELETE_ONLY;
            case 13:
                return SOURCE_ABSENT;
            case 14:
                return FINISH_DB_MIG;
            case 15:
                return DB_READONLY;
            case 16:
                return PUBLIC;
            default:
                return null;
            }
        }

        public int getValue() {
            return value;
        }

        public static final EnumSet<ComplexTaskStatus> ALL =
            EnumSet.allOf(ComplexTaskStatus.class);

        public static final EnumSet<ComplexTaskStatus> WRITABLE =
            EnumSet.of(WRITE_ONLY, WRITE_REORG, READY_TO_PUBLIC);

        public static final EnumSet<ComplexTaskStatus> NEED_SWITCH_DATASOURCE =
            EnumSet.of(SOURCE_WRITE_ONLY, SOURCE_DELETE_ONLY, SOURCE_ABSENT);

        public boolean isWriteOnly() {
            return WRITE_ONLY == this;
        }

        public boolean isWriteReorg() {
            return WRITE_REORG == this;
        }

        public boolean isDeleteOnly() {
            return DELETE_ONLY == this;
        }

        public boolean isReadyToPublic() {
            return READY_TO_PUBLIC == this;
        }

        public boolean isWritable() {
            return WRITABLE.contains(this);
        }

        public boolean isNeedSwitchDatasource() {
            return NEED_SWITCH_DATASOURCE.contains(this);
        }

        public boolean isReadOnly() {
            return DB_READONLY == this;
        }

        public boolean isPublic() {
            return PUBLIC == this;
        }
    }

    public static Map<String, List<ComplexTaskOutlineRecord>> getUnFinishTasksBySchName(String schemaName) {
        try (Connection conn = MetaDbUtil.getConnection()) {
            Map<String, List<ComplexTaskOutlineRecord>> tasks = new TreeMap<>(String::compareToIgnoreCase);
            ComplexTaskOutlineAccessor complexTaskOutlineAccessor = new ComplexTaskOutlineAccessor();
            complexTaskOutlineAccessor.setConnection(conn);
            List<ComplexTaskOutlineRecord> complexTaskOutlineRecords =
                complexTaskOutlineAccessor.getAllUnFinishComplexTaskBySch(schemaName);
            if (GeneralUtil.isNotEmpty(complexTaskOutlineRecords)) {
                List<ComplexTaskOutlineRecord> parentTasks =
                    complexTaskOutlineRecords.stream().filter(o -> o.getSubTask() == 0).collect(Collectors.toList());
                List<ComplexTaskOutlineRecord> tableTasks =
                    complexTaskOutlineRecords.stream().filter(o -> o.getSubTask() == 1).collect(Collectors.toList());
                for (ComplexTaskOutlineRecord subTask : tableTasks) {
                    tasks.computeIfAbsent(subTask.getObjectName(), o -> new ArrayList<>()).add(subTask);
                    tasks.get(subTask.getObjectName())
                        .addAll(parentTasks.stream().filter(o -> o.getJob_id() == subTask.getJob_id()).collect(
                            Collectors.toList()));
                }
            }
            return tasks;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION,
                e,
                "cannot get table info from system table");
        }
    }

    public static List<ComplexTaskOutlineRecord> getScaleOutTasksBySchName(String schemaName) {
        try (Connection conn = MetaDbUtil.getConnection()) {
            ComplexTaskOutlineAccessor complexTaskOutlineAccessor = new ComplexTaskOutlineAccessor();
            complexTaskOutlineAccessor.setConnection(conn);
            List<ComplexTaskOutlineRecord> complexTaskOutlineRecords =
                complexTaskOutlineAccessor
                    .getAllScaleOutComplexTaskBySch(ComplexTaskType.MOVE_DATABASE.getValue(), schemaName);

            return complexTaskOutlineRecords;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION,
                e,
                "cannot get table info from system table");
        }
    }

    public static ComplexTaskTableMetaBean getComplexTaskTableMetaBean(String schemaName,
                                                                       String logicalTableName,
                                                                       List<ComplexTaskOutlineRecord> parentTasks,
                                                                       List<ComplexTaskOutlineRecord> subTasks) {
        ComplexTaskTableMetaBean complexTaskTableMetaBean =
            new ComplexTaskTableMetaBean(schemaName, logicalTableName);
        for (ComplexTaskOutlineRecord parentTask : parentTasks) {
            for (ComplexTaskOutlineRecord subTask : subTasks) {
                if (subTask.getJob_id() == parentTask.getJob_id()) {
                    complexTaskTableMetaBean.getPartitionTableMetaMap()
                        .put(parentTask.getObjectName(), ComplexTaskStatus.from(subTask.getStatus()));
                    ParentComplexTaskStatusInfo parentComplexTaskStatusInfo =
                        new ParentComplexTaskStatusInfo(parentTask.getTableGroupName(), parentTask.getObjectName(),
                            ComplexTaskStatus.from(parentTask.getStatus()), ComplexTaskType.from(parentTask.getType()));
                    complexTaskTableMetaBean.getParentComplexTaskStatusInfoMap()
                        .put(parentTask.getObjectName(), parentComplexTaskStatusInfo);
                    break;
                }
            }
        }

        return complexTaskTableMetaBean;
    }

    public static ComplexTaskTableMetaBean getComplexTaskTableMetaBean(String schemaName,
                                                                       String logicalTableName,
                                                                       List<ComplexTaskOutlineRecord> allTasks) {
        if (GeneralUtil.isEmpty(allTasks)) {
            return new ComplexTaskTableMetaBean(schemaName, logicalTableName);
        }
        List<ComplexTaskOutlineRecord> parentTasks = allTasks.stream().filter(o -> o.getSubTask() == 0).collect(
            Collectors.toList());
        List<ComplexTaskOutlineRecord> subTasks = allTasks.stream().filter(o -> o.getSubTask() == 1).collect(
            Collectors.toList());
        return getComplexTaskTableMetaBean(schemaName, logicalTableName, parentTasks, subTasks);
    }

    public static ComplexTaskTableMetaBean getComplexTaskTableMetaBean(String schemaName,
                                                                       String logicalTableName) {
        try (Connection conn = MetaDbUtil.getConnection()) {
            return getComplexTaskTableMetaBean(conn, schemaName, logicalTableName);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION,
                e,
                "cannot get table info from system table");
        }
    }

    public static ComplexTaskTableMetaBean getComplexTaskTableMetaBean(Connection metaDbConn,
                                                                       String schemaName,
                                                                       String logicalTableName) {
        if (metaDbConn == null) {
            return getComplexTaskTableMetaBean(schemaName, logicalTableName);
        }

        ComplexTaskOutlineAccessor complexTaskOutlineAccessor = new ComplexTaskOutlineAccessor();
        complexTaskOutlineAccessor.setConnection(metaDbConn);
        List<ComplexTaskOutlineRecord> complexTaskOutlineRecords =
            complexTaskOutlineAccessor.getAllUnFinishComlexTaskBySchemaAndTable(schemaName, logicalTableName);
        if (GeneralUtil.isEmpty(complexTaskOutlineRecords)) {
            return new ComplexTaskTableMetaBean(schemaName, logicalTableName);
        } else {
            return getComplexTaskTableMetaBean(schemaName, logicalTableName,
                complexTaskOutlineRecords.stream().filter(o -> o.getSubTask() == 0)
                    .collect(Collectors.toList()),
                complexTaskOutlineRecords.stream().filter(o -> o.getSubTask() == 1)
                    .collect(Collectors.toList()));
        }
    }

    public static List<ComplexTaskOutlineRecord> getAllUnFinishParentComlexTask(String schemaName) {

        try (Connection conn = MetaDbUtil.getConnection()) {
            ComplexTaskOutlineAccessor complexTaskOutlineAccessor = new ComplexTaskOutlineAccessor();
            complexTaskOutlineAccessor.setConnection(conn);
            return complexTaskOutlineAccessor.getAllUnFinishParentComlexTask(schemaName);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION,
                e,
                "cannot get table info from system table");
        }
    }

    //1:split, 2:merge, 3:move
    public enum ComplexTaskType {
        SPLIT_PARTITION(1),
        MERGE_PARTITION(2),
        MOVE_PARTITION(3),
        EXTRACT_PARTITION(4),
        SET_TABLEGROUP(5),
        ADD_PARTITION(6),
        DROP_PARTITION(7),
        MODIFY_PARTITION(8),
        REFRESH_TOPOLOGY(9),
        MOVE_DATABASE(10),
        SPLIT_HOT_VALUE(11);

        private final int value;

        public static ComplexTaskType from(int value) {
            switch (value) {
            case 1:
                return SPLIT_PARTITION;
            case 2:
                return MERGE_PARTITION;
            case 3:
                return MOVE_PARTITION;
            case 4:
                return EXTRACT_PARTITION;
            case 5:
                return SET_TABLEGROUP;
            case 6:
                return ADD_PARTITION;
            case 7:
                return DROP_PARTITION;
            case 8:
                return MODIFY_PARTITION;
            case 9:
                return REFRESH_TOPOLOGY;
            case 10:
                return MOVE_DATABASE;
            case 11:
                return SPLIT_HOT_VALUE;
            default:
                return null;
            }
        }

        ComplexTaskType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    static class ParentComplexTaskStatusInfo {
        //for movedatabase tableGroupName is null
        protected final String tableGroupName;
        //ojectName is sourceGroup for movedatabase
        //ojectName is partitionGroup for alter tablegroup
        protected final String objectName;
        protected final ComplexTaskMetaManager.ComplexTaskStatus status;
        protected final ComplexTaskType taskType;

        public ParentComplexTaskStatusInfo(String tableGroupName, String objectName,
                                           ComplexTaskMetaManager.ComplexTaskStatus status,
                                           ComplexTaskType taskType) {
            this.tableGroupName = tableGroupName;
            this.objectName = objectName;
            this.status = status;
            this.taskType = taskType;
        }
    }

    @Override
    protected void doDestroy() {
        MetaDbConfigManager.getInstance().unbindListener(MetaDbDataIdBuilder.getDbComplexTaskDataId(schemaName));
    }

    public class ComplexTaskMetaListener implements ConfigListener {

        protected String schemaName;

        public ComplexTaskMetaListener(String schemaName) {
            this.schemaName = schemaName;
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            LoggerInit.TDDL_DYNAMIC_CONFIG
                .info("[Data Received] [COMPLEX_TASK_META] update complex task status for " + dataId);
            reloadComplexTaskStatus();
        }
    }

    public void reloadComplexTaskStatus() {
        ComplexTaskOutlineAccessor complexTaskOutlineAccessor = new ComplexTaskOutlineAccessor();
        try (Connection conn = MetaDbUtil.getConnection()) {
            complexTaskOutlineAccessor.setConnection(conn);
            List<ComplexTaskOutlineRecord> records = complexTaskOutlineAccessor
                .getAllUnFinishParentComlexTask(schemaName);

            if (GeneralUtil.isNotEmpty(records)) {
                for (ComplexTaskOutlineRecord record : records) {
                    ParentComplexTaskStatusInfo parentComplexTaskStatusInfo =
                        new ParentComplexTaskStatusInfo(record.getTableGroupName(), record.getObjectName(),
                            ComplexTaskStatus.from(record.getStatus()),
                            ComplexTaskType.from(record.getType()));
                    String tableGroupName =
                        StringUtils.isEmpty(record.getTableGroupName()) ? "" : record.getTableGroupName();
                    String key = tableGroupName + "#" + record.getObjectName();
                    complexTaskStatusInfoMap.put(key, parentComplexTaskStatusInfo);
                }
            } else {
                complexTaskStatusInfoMap.clear();
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION,
                e,
                "cannot get table info from system table");
        }
    }

    public static class ComplexTaskTableMetaBean {
        final String schemaName;
        final String logicalTableName;
        //key:partitionGroup or sourceDBGroup, val:tableStatus
        final Map<String, ComplexTaskStatus> partitionTableMetaMap;
        //key:partitionGroup or sourceDBGroup, val:ParentComplexTaskStatusInfo
        final Map<String, ParentComplexTaskStatusInfo> parentComplexTaskStatusInfoMap;

        public ComplexTaskTableMetaBean(String schemaName, String logicalTableName) {
            this.schemaName = schemaName;
            this.logicalTableName = logicalTableName;
            this.partitionTableMetaMap = new TreeMap<>(String::compareToIgnoreCase);
            this.parentComplexTaskStatusInfoMap = new TreeMap<>(String::compareToIgnoreCase);
        }

        public boolean isNeedSwitchDatasource() {
            if (GeneralUtil.isEmpty(parentComplexTaskStatusInfoMap)) {
                return false;
            } else {
                for (Map.Entry<String, ParentComplexTaskStatusInfo> entry : parentComplexTaskStatusInfoMap.entrySet()) {
                    if (entry.getValue().status.isNeedSwitchDatasource()) {
                        return true;
                    }
                }
            }
            return false;
        }

        public boolean canWrite() {
            if (GeneralUtil.isEmpty(partitionTableMetaMap)) {
                return false;
            } else {
                for (Map.Entry<String, ComplexTaskStatus> entry : partitionTableMetaMap.entrySet()) {
                    if (entry.getValue().isDeleteOnly() || entry.getValue().isWritable()) {
                        return true;
                    }
                }
            }
            return false;
        }

        public boolean canWrite(String partitionName) {
            if (GeneralUtil.isEmpty(partitionTableMetaMap) || partitionTableMetaMap.get(partitionName) == null) {
                return false;
            } else {
                return partitionTableMetaMap.get(partitionName).isWritable() || partitionTableMetaMap.get(partitionName)
                    .isDeleteOnly();
            }
        }

        public boolean isDeleteOnly(String partitionName) {
            if (GeneralUtil.isEmpty(partitionTableMetaMap) || partitionTableMetaMap.get(partitionName) == null) {
                return false;
            } else {
                return partitionTableMetaMap.get(partitionName).isDeleteOnly();
            }
        }

        public boolean isReadyToPulic(String partitionName) {
            if (GeneralUtil.isEmpty(partitionTableMetaMap) || partitionTableMetaMap.get(partitionName) == null) {
                return false;
            } else {
                return partitionTableMetaMap.get(partitionName).isReadyToPublic();
            }
        }

        public boolean allPartIsDeleteOnly() {
            if (GeneralUtil.isEmpty(partitionTableMetaMap)) {
                return false;
            } else {
                for (Map.Entry<String, ComplexTaskStatus> entry : partitionTableMetaMap.entrySet()) {
                    if (!entry.getValue().isDeleteOnly()) {
                        return false;
                    }
                }
            }
            return true;
        }

        public boolean allPartIsReadyToPublic() {
            if (GeneralUtil.isEmpty(partitionTableMetaMap)) {
                return false;
            } else {
                for (Map.Entry<String, ComplexTaskStatus> entry : partitionTableMetaMap.entrySet()) {
                    if (!entry.getValue().isReadyToPublic()) {
                        return false;
                    }
                }
            }
            return true;
        }

        public boolean allPartIsPublic() {
            if (GeneralUtil.isEmpty(partitionTableMetaMap)) {
                return true;
            } else {
                for (Map.Entry<String, ComplexTaskStatus> entry : partitionTableMetaMap.entrySet()) {
                    if (!entry.getValue().isPublic()) {
                        return false;
                    }
                }
            }
            return true;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public String getLogicalTableName() {
            return logicalTableName;
        }

        public Map<String, ComplexTaskStatus> getPartitionTableMetaMap() {
            return partitionTableMetaMap;
        }

        public Map<String, ParentComplexTaskStatusInfo> getParentComplexTaskStatusInfoMap() {
            return parentComplexTaskStatusInfoMap;
        }

        public String getDigest() {
            StringBuilder sb = new StringBuilder();
            sb.append("ComplexTaskTableMetaBeanDigest:[");
            sb.append(this.logicalTableName);
            sb.append(":");
            try {
                if (GeneralUtil.isNotEmpty(partitionTableMetaMap)) {
                    for (Map.Entry<String, ComplexTaskStatus> entry : partitionTableMetaMap.entrySet()) {
                        sb.append("[");
                        sb.append(entry.getKey());
                        sb.append(":");
                        sb.append(entry.getValue().toString());
                        sb.append("]\n");
                    }
                }
                if (GeneralUtil.isNotEmpty(parentComplexTaskStatusInfoMap)) {
                    for (Map.Entry<String, ParentComplexTaskStatusInfo> entry : parentComplexTaskStatusInfoMap
                        .entrySet()) {
                        sb.append("[");
                        sb.append(entry.getKey());
                        sb.append(":");
                        sb.append(entry.getValue().tableGroupName);
                        sb.append(",");
                        sb.append(entry.getValue().objectName);
                        sb.append(",");
                        sb.append(entry.getValue().status.toString());
                        sb.append("]\n");
                    }
                }
                sb.append("isNeedSwitchDatasource:");
                sb.append(this.isNeedSwitchDatasource());
                sb.append("]");
            } catch (Exception ex) {
                //ignore
            }
            return sb.toString();
        }
    }

    public static void insertComplexTask(ComplexTaskOutlineRecord complexTaskOutlineRecord,
                                         Connection metaDbConnection) {
        ComplexTaskOutlineAccessor complexTaskOutlineAccessor = new ComplexTaskOutlineAccessor();
        complexTaskOutlineAccessor.setConnection(metaDbConnection);
        complexTaskOutlineAccessor.addNewComplexTaskOutline(complexTaskOutlineRecord);
    }

    public static void deleteComplexTaskByJobIdAndObjName(Long jobId, String schemaName, String objectName,
                                                          Connection metaDbConnection) {
        ComplexTaskOutlineAccessor complexTaskOutlineAccessor = new ComplexTaskOutlineAccessor();
        complexTaskOutlineAccessor.setConnection(metaDbConnection);
        complexTaskOutlineAccessor.deleteComplexTaskByJobIdAndObjName(jobId, schemaName, objectName);
    }

    public static void updateAllSubTasksStatusByJobId(Long jobId, String schemaName, ComplexTaskStatus before,
                                                      ComplexTaskStatus after,
                                                      Connection connection) {
        ComplexTaskOutlineAccessor accessor = new ComplexTaskOutlineAccessor();
        accessor.setConnection(connection);
        accessor.updateAllSubTasksStatusByJobId(jobId, schemaName, before.getValue(), after.getValue());
    }

    public static void updateParentComplexTaskStatusByJobId(Long jobId, String schemaName, ComplexTaskStatus oldStatus,
                                                            ComplexTaskStatus newStatus,
                                                            Connection connection) {
        ComplexTaskOutlineAccessor accessor = new ComplexTaskOutlineAccessor();
        accessor.setConnection(connection);
        accessor.updateParentComplexTaskStatusByJobId(jobId, schemaName, oldStatus.getValue(), newStatus.getValue());
    }

    public static void updateSubTasksStatusByJobIdAndObjName(Long jobId, String schemaName,
                                                             String objectName,
                                                             ComplexTaskStatus oldStatus,
                                                             ComplexTaskStatus newStatus,
                                                             Connection connection) {
        ComplexTaskOutlineAccessor accessor = new ComplexTaskOutlineAccessor();
        accessor.setConnection(connection);
        accessor.updateSubTasksStatusByJobIdAndObjName(jobId, schemaName, objectName, oldStatus.getValue(),
            newStatus.getValue());
    }
}
