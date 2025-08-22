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

package com.alibaba.polardbx.common.cdc;

import java.util.List;
import java.util.Set;

public interface ICdcManager {

    String TABLE_NEW_NAME = "CDC.TABLE_NEW_NAME";

    String TABLE_NEW_PATTERN = "CDC.TABLE_NEW_PATTERN";

    String ALTER_TRIGGER_TOPOLOGY_CHANGE_FLAG = "ALTER_TRIGGER_TOPOLOGY_CHANGE_FLAG";

    String NOT_IGNORE_GSI_JOB_TYPE_FLAG = "NOT_IGNORE_GSI_JOB_TYPE_FLAG";

    /**
     * 通知CdcManager，是否在打标记录中重新构建为CDC Meta模块提供的物理表建表SQL
     */
    String REFRESH_CREATE_SQL_4_PHY_TABLE = "REFRESH_CREATE_SQL_4_PHY_TABLE";

    /**
     * 通知CdcManager， 是否是标记 ORIGINAL_DDL;
     */
    String USE_ORIGINAL_DDL = "USE_ORIGINAL_DDL";
    /**
     * 标识Foreign Keys DDL
     */
    String FOREIGN_KEYS_DDL = "FOREIGN_KEYS_DDL";

    /**
     * 是否在 ORIGINAL_DDL 中增加 DDL_ID
     */
    String USE_DDL_VERSION_ID = "USE_DDL_VERSION_ID";

    Long DEFAULT_DDL_VERSION_ID = -1L;

    /**
     * 是否使用OMC
     */
    String USE_OMC = "USE_OMC";

    /**
     * support multi mark in one task
     */
    String TASK_MARK_SEQ = "TASK_MARK_SEQ";

    String CDC_MARK_SQL_MODE = "cdc_mark_sql_mode";

    String CDC_ORIGINAL_DDL = "original_ddl";
    String CDC_IS_GSI = "CDC_IS_GSI";
    String CDC_IS_CCI = "CDC_IS_CCI";
    String CDC_GSI_PRIMARY_TABLE = "CDC_GSI_PRIMARY_TABLE";
    String CDC_GROUP_NAME = "cdc_group_name";
    String CDC_ACTUAL_ALTER_TABLE_GROUP_FLAG = "cdc_actual_alter_table_group_flag";
    String CDC_TABLE_GROUP_MANUAL_CREATE_FLAG = "cdc_table_group_manual_create_flag";
    String CDC_DDL_SCOPE = "cdc_ddl_scope";
    String POLARDBX_SERVER_ID = "polardbx_server_id";
    String SQL_LOG_BIN = "sql_log_bin";
    String DDL_ID = "DDL_ID";
    String EXCHANGE_NAMES_MAPPING = "EXCHANGE_NAMES_MAPPING";
    String CDC_MARK_RECORD_COMMIT_TSO = "cdc_mark_record_commit_tso";
    String CDC_ARCHIVE_DROP_PARTITION = "CDC_ARCHIVE_DROP_PARTITION";

    /**
     * 发送Cdc通用指令
     */
    void sendInstruction(InstructionType instructionType, String instructionId, String instructionContent);

    void notifyDdl(CdcDDLContext cdcDDLContext);

    void syncToLeaderMarkDdl(CdcDDLContext cdcDDLContext);

    /**
     * 查询用于 DDL 打标的 CDC 系统表记录
     */
    List<CdcDdlRecord> getDdlRecord(CdcDDLContext cdcDdlContext);

    /**
     * make sure cdc has receive storage change instruction before removing storage。
     */
    void checkCdcBeforeStorageRemove(Set<String> storageInstIds, String identifier);

    enum InstructionType {
        /**
         * Cdc初始化
         */
        CdcStart,
        /**
         * 存储实例发生了变更
         */
        StorageInstChange,
        /**
         * 元数据镜像
         */
        MetaSnapshot;
    }
}
